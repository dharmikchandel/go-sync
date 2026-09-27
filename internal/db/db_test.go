package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dharmikchandel/go-sync/internal/db"
	"github.com/dharmikchandel/go-sync/internal/testenv"
)

// Several replicas starting at once must not corrupt the schema: the
// migration lock should serialize them and each should succeed.
func TestMigrateConcurrentReplicas(t *testing.T) {
	url := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Go(func() {
			pool, err := db.Connect(ctx, url)
			if err != nil {
				errs <- err
				return
			}
			defer pool.Close()
			errs <- db.Migrate(ctx, pool)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
}

func TestConnectGivesUpWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := db.Connect(ctx, "postgres://nobody@127.0.0.1:1/none?sslmode=disable")
	if err == nil {
		t.Fatal("expected an error connecting to a closed port")
	}
}
