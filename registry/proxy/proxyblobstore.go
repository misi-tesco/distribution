package proxy

import (
	"context"
	"errors"
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

type BlobFetch struct {
	complete       bool
	completeCond   *sync.Cond
	readableWriter *storage.ReadableWriter
	mutex          sync.Mutex
}

func NewBlobFetch(w *storage.ReadableWriter) *BlobFetch {
	res := &BlobFetch{
		readableWriter: w,
	}
	res.completeCond = sync.NewCond(&res.mutex)
	return res
}

// inflight tracks currently downloading blobs
var inflight = make(map[digest.Digest]*BlobFetch)

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

func (pbs *proxyBlobStore) FetchFromRemote(dgst digest.Digest, ctx context.Context, fetcher *BlobFetch) {
	err := pbs.doFetchFromRemote(dgst, ctx, fetcher.readableWriter)
	if err != nil {
		log.Printf("Failed to fetch layer %s, error: %s", dgst, err)
	}
}

func (pbs *proxyBlobStore) doFetchFromRemote(dgst digest.Digest, ctx context.Context, bw *storage.ReadableWriter) error {
	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		(*bw).CancelWithError(ctx, err)
		return err
	}

	desc, err := pbs.copyContent(ctx, dgst, *bw)
	mu.Lock()
	inflight[dgst] = nil
	defer mu.Unlock()
	if err != nil {
		(*bw).CancelWithError(ctx, err)
		dcontext.GetLogger(ctx).Errorf(" Error copying to storage: %s", err.Error())
		return err
	}

	_, err = (*bw).Commit(ctx, desc)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("Error committing to storage: %s", err.Error())
		return err
	}

	blobRef, err := reference.WithDigest(pbs.repositoryName, dgst)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("Error creating reference: %s", err)
		return err
	}

	pbs.scheduler.AddBlob(blobRef, repositoryTTL)
	return nil
}

func (pbs *proxyBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	mu.Lock()
	infl, ok := inflight[dgst]
	isPresent := pbs.IsPresentLocally(ctx, dgst)
	log.Printf("digest %s present: %t, in flight: %t, in flight object: %v", dgst, isPresent, ok, infl)
	isNew := !isPresent && !ok
	if isNew {
		bw, err := pbs.localStore.Create(ctx)
		if err != nil {
			mu.Unlock()
			return err
		}
		rw, _ := bw.(storage.ReadableWriter)
		infl = NewBlobFetch(&rw)
		inflight[dgst] = infl
		go pbs.FetchFromRemote(dgst, ctx, infl)
	}

	inflightReader := io.ReadCloser(nil)
	var err error
	if infl != nil {
		inflightReader, err = (*infl.readableWriter).Reader()
	}
	mu.Unlock()
	if err != nil {
		return err
	}
	if !isNew {
		//proxyMetrics.BlobHits()
	}
	if isPresent {
		err := pbs.doServeFromLocalStore(ctx, w, r, dgst)
		if err != nil {
			dcontext.GetLogger(ctx).Errorf("Error serving blob from local storage: %s", err.Error())
			return err
		}
		log.Printf("Served %s from local storage, repo: %s", dgst, pbs.repositoryName)
		log.Printf("Sum of pushed data is %d", proxyMetrics.blobMetrics.BytesPushed)
		return nil
	} else {
		if inflightReader == nil {
			// this should not be reached, local stream should always be readable
			return errors.New("unable to read blob being written")
		}
		size, err := io.Copy(w, inflightReader)
		if err != nil {
			log.Printf("Error occurred while fetching from in-flight reader: %s", err)
			return err
		}
		desc, err := pbs.localStore.Stat(ctx, dgst)
		if err != nil {
			return err
		}
		if desc.Size != size {
			log.Printf("Size did not match for %s. Expected: %d, downloaded: %d", dgst, desc.Size, size)
			return io.EOF
		}
		return nil
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
