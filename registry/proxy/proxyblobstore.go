package proxy

import (
	"context"
	"github.com/distribution/distribution/v3/registry/storage"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/distribution/distribution/v3"
	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/reference"
	"github.com/distribution/distribution/v3/registry/proxy/scheduler"
	"github.com/opencontainers/go-digest"
)

type proxyBlobStore struct {
	localStore     distribution.BlobStore
	remoteStore    distribution.BlobService
	scheduler      *scheduler.TTLExpirationScheduler
	repositoryName reference.Named
	authChallenger authChallenger
}

var _ distribution.BlobStore = &proxyBlobStore{}

type InFlight struct {
	downloading    bool
	lastError      error
	readableWriter storage.ReadableWriter
}

// inflight tracks currently downloading blobs
var inflight = make(map[digest.Digest]*InFlight)

// mu protects inflight
var mu sync.Mutex
var cond = sync.NewCond(&mu)

func setResponseHeaders(w http.ResponseWriter, length int64, mediaType string, digest digest.Digest) {
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", digest.String())
	w.Header().Set("Etag", digest.String())
}

func (pbs *proxyBlobStore) copyContent(ctx context.Context, dgst digest.Digest, writer io.Writer) (distribution.Descriptor, error) {
	desc, err := pbs.remoteStore.Stat(ctx, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if w, ok := writer.(http.ResponseWriter); ok {
		setResponseHeaders(w, desc.Size, desc.MediaType, dgst)
	}

	remoteReader, err := pbs.remoteStore.Open(ctx, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	defer remoteReader.Close()

	_, err = io.CopyN(writer, remoteReader, desc.Size)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	proxyMetrics.BlobPush(uint64(desc.Size))

	return desc, nil
}

func (pbs *proxyBlobStore) serveLocal(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (bool, error) {
	localDesc, err := pbs.localStore.Stat(ctx, dgst)
	if err != nil {
		// Stat can report a zero sized file here if it's checked between creation
		// and population.  Return nil error, and continue
		return false, nil
	}

	proxyMetrics.BlobPush(uint64(localDesc.Size))
	return true, pbs.localStore.ServeBlob(ctx, w, r, dgst)
}

func (pbs *proxyBlobStore) doServeFromLocalStore(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	log.Printf("Serving %s from local storage, repo: %s", dgst, pbs.repositoryName)
	_, err := pbs.localStore.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	//proxyMetrics.BlobServe(uint64(localDesc.Size))
	return pbs.localStore.ServeBlob(ctx, w, r, dgst)

}

func (pbs *proxyBlobStore) IsPresentLocally(ctx context.Context, dgst digest.Digest) bool {
	_, err := pbs.localStore.Stat(ctx, dgst)
	// Stat can report a zero sized file here if it's checked between creation
	// and population.  Return nil error, and continue
	return err == nil
}

func (pbs *proxyBlobStore) storeLocal(ctx context.Context, dgst digest.Digest, bw distribution.BlobWriter) error {
	var desc distribution.Descriptor
	var err error

	desc, err = pbs.copyContent(ctx, dgst, bw)
	if err != nil {
		bw.Cancel(ctx)
		return err
	}

	_, err = bw.Commit(ctx, desc)
	if err != nil {
		return err
	}

	return nil
}

func (pbs *proxyBlobStore) FetchFromRemote(dgst digest.Digest, ctx context.Context, bw *distribution.BlobWriter) {
	defer func() {
		mu.Lock()
		inflight[dgst].downloading = false
		cond.Broadcast()
		mu.Unlock()
	}()

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		inflight[dgst].lastError = err
		return
	}

	//err := pbs.storeLocal(ctx, dgst, bw)

	desc, err := pbs.copyContent(ctx, dgst, *bw)
	if err != nil {
		(*bw).Cancel(ctx)
		dcontext.GetLogger(ctx).Errorf("Error copying to storage: %s", err.Error())
		inflight[dgst].lastError = err
		return
	}

	mu.Lock()
	_, err = (*bw).Commit(ctx, desc)
	mu.Unlock()
	if err != nil {
		inflight[dgst].lastError = err
		dcontext.GetLogger(ctx).Errorf("Error committing to storage: %s", err.Error())
		return
	}

	blobRef, err := reference.WithDigest(pbs.repositoryName, dgst)
	if err != nil {
		inflight[dgst].lastError = err
		dcontext.GetLogger(ctx).Errorf("Error creating reference: %s", err)
		return
	}

	pbs.scheduler.AddBlob(blobRef, repositoryTTL)
}

func (pbs *proxyBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	mu.Lock()
	infl, ok := inflight[dgst]
	isPresent := pbs.IsPresentLocally(ctx, dgst)
	log.Printf("digest %s present: %t, in flight: %t, in flight object: %v", dgst, isPresent, ok, infl)
	isNew := !isPresent && (!ok || infl.downloading == false)
	if isNew {
		if !ok {
			infl = &InFlight{
				lastError: nil,
			}
			inflight[dgst] = infl
		}
		infl.downloading = true
		bw, err := pbs.localStore.Create(ctx)
		if err != nil {
			mu.Unlock()
			return err
		}
		rw, ok := bw.(storage.ReadableWriter)
		if ok {
			infl.readableWriter = rw
		}
		go pbs.FetchFromRemote(dgst, ctx, &bw)
	}

	inflightReader := io.ReadCloser(nil)
	var err error
	if infl != nil && infl.downloading && infl.readableWriter != nil {
		inflightReader, err = infl.readableWriter.Reader()
	}

	mu.Unlock()
	if err != nil {
		return err
	}
	if inflightReader != nil {
		size, err := io.Copy(w, inflightReader)
		if err != nil {
			return err
		}
		desc, err := pbs.localStore.Stat(ctx, dgst)
		if err != nil {
			return err
		}
		if desc.Size != size {
			return io.EOF
		}
		return nil
	}

	for isDownloading := infl != nil && infl.downloading; isDownloading && !isPresent; {
		log.Printf("Waiting for background download to finish for %s ..", dgst)
		cond.Wait()
		isPresent = pbs.IsPresentLocally(ctx, dgst)
		isDownloading = infl != nil && infl.downloading
	}

	var lastError error = nil
	if infl != nil {
		lastError = infl.lastError
	}

	if isPresent {
		err := pbs.doServeFromLocalStore(ctx, w, r, dgst)
		if err != nil {
			dcontext.GetLogger(ctx).Errorf("Error serving blob from local storage: %s", err.Error())
			return err
		}
		if !isNew {
			//proxyMetrics.BlobHits()
		}
		log.Printf("Served %s from local storage, repo: %s", dgst, pbs.repositoryName)
		log.Printf("Sum of pushed data is %d", proxyMetrics.blobMetrics.BytesPushed)
		return nil
	}
	if lastError != nil {
		return lastError
	} else {
		log.Printf("the requested blob %s is not found, repo: %s", dgst, pbs.repositoryName)
		return distribution.ErrBlobUnknown
	}
}

func (pbs *proxyBlobStore) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	desc, err := pbs.localStore.Stat(ctx, dgst)
	if err == nil {
		return desc, err
	}

	if err != distribution.ErrBlobUnknown {
		return distribution.Descriptor{}, err
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return distribution.Descriptor{}, err
	}

	return pbs.remoteStore.Stat(ctx, dgst)
}

func (pbs *proxyBlobStore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	blob, err := pbs.localStore.Get(ctx, dgst)
	if err == nil {
		return blob, nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return []byte{}, err
	}

	blob, err = pbs.remoteStore.Get(ctx, dgst)
	if err != nil {
		return []byte{}, err
	}

	_, err = pbs.localStore.Put(ctx, "", blob)
	if err != nil {
		return []byte{}, err
	}
	return blob, nil
}

// Unsupported functions
func (pbs *proxyBlobStore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	return distribution.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Mount(ctx context.Context, sourceRepo reference.Named, dgst digest.Digest) (distribution.Descriptor, error) {
	return distribution.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Open(ctx context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return distribution.ErrUnsupported
}
