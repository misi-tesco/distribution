package filesystem

import (
	"github.com/distribution/distribution/v3/context"
	"io/ioutil"
	"math/rand"
	"testing"
)

func TestBlobWritePipe(t *testing.T) {
	cacheDir, err := ioutil.TempDir("", "cache")
	if err != nil {
		t.Fatalf("unable to create tempdir: %s", err)
	}

	driver, err := FromParameters(map[string]interface{}{
		"rootdirectory": cacheDir,
	})
	if err != nil {
		t.Fatalf("unable to create filesystem driver: %s", err)
	}
	ctx := context.Background()
	_, err = driver.Writer(ctx, "file1", false)
	if err != nil {
		t.Fatal(err)
	}

}

func makeBlob(size int) []byte {
	blob := make([]byte, size)
	for i := 0; i < size; i++ {
		blob[i] = byte('A' + rand.Int()%48)
	}
	return blob
}
