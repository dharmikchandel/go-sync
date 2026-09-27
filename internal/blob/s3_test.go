package blob_test

import (
	"context"
	"testing"
	"time"

	"github.com/dharmikchandel/go-sync/internal/blob"
	"github.com/dharmikchandel/go-sync/internal/testenv"
)

// Opening twice must succeed: the second open finds the bucket already there,
// which is what happens every time a replica restarts.
func TestOpenCreatesBucketIdempotently(t *testing.T) {
	m := testenv.ObjectStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := blob.Options{Endpoint: m.Endpoint, AccessKey: m.AccessKey, SecretKey: m.SecretKey, Bucket: "test-bucket"}
	for i := range 2 {
		if _, err := blob.Open(ctx, opts); err != nil {
			t.Fatalf("open #%d: %v", i+1, err)
		}
	}
}
