// Package testenv starts real Postgres and S3 (RustFS) containers for integration
// tests. Mocks can't prove behaviour that depends on the real systems (row
// locking, bucket semantics), so integration tests use the real thing.
package testenv

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dharmikchandel/go-sync/internal/db"
)

// Postgres starts a throwaway Postgres and returns its connection URL.
func Postgres(t *testing.T) string {
	t.Helper()
	skipIfShort(t)
	requireDocker(t)
	ctx := context.Background()

	c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("gosync"),
		tcpostgres.WithUsername("gosync"),
		tcpostgres.WithPassword("gosync"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return url
}

// S3Image is pinned so tests and docker-compose run the same server.
const S3Image = "rustfs/rustfs:1.0.0"

type S3 struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

func startObjectStore(t *testing.T) (S3, error) {
	t.Helper()
	skipIfShort(t)
	requireDocker(t)
	ctx := context.Background()
	creds := S3{AccessKey: "gosync", SecretKey: "gosync-secret"}

	c, err := testcontainers.Run(ctx, S3Image,
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"RUSTFS_VOLUMES":    "/data",
			"RUSTFS_ACCESS_KEY": creds.AccessKey,
			"RUSTFS_SECRET_KEY": creds.SecretKey,
		}),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/health").WithPort("9000/tcp")),
	)
	if err != nil {
		return S3{}, err
	}
	creds.Endpoint, err = c.PortEndpoint(ctx, "9000/tcp", "")
	return creds, err
}

// requireDocker skips the test if Docker isn't available, unless
// GOSYNC_REQUIRE_DOCKER=1 (set in CI), where a skip would hide that the
// integration tests never ran, so it fails instead.
func requireDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("GOSYNC_REQUIRE_DOCKER") == "1" {
		if _, err := testcontainers.NewDockerClientWithOpts(context.Background()); err != nil {
			t.Fatalf("Docker is required (GOSYNC_REQUIRE_DOCKER=1): %v", err)
		}
		return
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (run without -short)")
	}
}

var (
	sharedPG     sync.Once
	sharedPGURL  string
	sharedPGErr  error
	sharedS3     sync.Once
	sharedS3Info S3
	sharedS3Err  error
)

// Pool returns a connection pool to a migrated Postgres shared by every test
// in the package. Tests isolate themselves by using their own user (see
// User), which is cheaper than a container per test. The container is
// removed when the test binary exits.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	skipIfShort(t)
	requireDocker(t)
	ctx := context.Background()

	sharedPG.Do(func() {
		var c *tcpostgres.PostgresContainer
		c, sharedPGErr = tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("gosync"),
			tcpostgres.WithUsername("gosync"),
			tcpostgres.WithPassword("gosync"),
			tcpostgres.BasicWaitStrategies(),
		)
		if sharedPGErr != nil {
			return
		}
		if sharedPGURL, sharedPGErr = c.ConnectionString(ctx, "sslmode=disable"); sharedPGErr != nil {
			return
		}
		var pool *pgxpool.Pool
		if pool, sharedPGErr = db.Connect(ctx, sharedPGURL); sharedPGErr != nil {
			return
		}
		defer pool.Close()
		sharedPGErr = db.Migrate(ctx, pool)
	})
	if sharedPGErr != nil {
		t.Fatalf("shared postgres: %v", sharedPGErr)
	}

	pool, err := db.Connect(ctx, sharedPGURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// SharedObjectStore returns an S3 server shared by every test in the package.
func SharedObjectStore(t *testing.T) S3 {
	t.Helper()
	sharedS3.Do(func() {
		// Borrow t for startup; the container outlives it on purpose and is
		// removed when the test binary exits.
		sharedS3Info, sharedS3Err = startObjectStore(t)
	})
	if sharedS3Err != nil {
		t.Fatalf("shared object store: %v", sharedS3Err)
	}
	return sharedS3Info
}

// User returns a user name unique to this test, so tests sharing a database
// can't see each other's files.
func User(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%d-%d", os.Getpid(), userSeq.Add(1))
}

var userSeq atomic.Int64
