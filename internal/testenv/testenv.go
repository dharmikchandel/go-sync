// Package testenv starts real Postgres and S3 (RustFS) containers for integration
// tests. Mocks can't prove behaviour that depends on the real systems (row
// locking, bucket semantics), so integration tests use the real thing.
package testenv

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Postgres starts a throwaway Postgres and returns its connection URL.
func Postgres(t *testing.T) string {
	t.Helper()
	skipIfShort(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
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

// ObjectStore starts a throwaway S3-compatible server (RustFS).
func ObjectStore(t *testing.T) S3 {
	t.Helper()
	skipIfShort(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
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
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start object store: %v", err)
	}
	creds.Endpoint, err = c.PortEndpoint(ctx, "9000/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (run without -short)")
	}
}
