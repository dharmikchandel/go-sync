// Command server runs the go-sync gRPC server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/dharmikchandel/go-sync/internal/blob"
	"github.com/dharmikchandel/go-sync/internal/config"
	"github.com/dharmikchandel/go-sync/internal/db"
	"github.com/dharmikchandel/go-sync/internal/meta"
	"github.com/dharmikchandel/go-sync/internal/server"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log = log.With("replica", cfg.ReplicaID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()

	pool, err := db.Connect(startCtx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(startCtx, pool); err != nil {
		return err
	}
	log.Info("postgres ready")

	blobs, err := blob.Open(startCtx, blob.Options{
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
		Bucket:    cfg.S3Bucket,
		UseTLS:    cfg.S3UseTLS,
	})
	if err != nil {
		return err
	}
	log.Info("object storage ready", "bucket", cfg.S3Bucket)

	srv := server.New(server.Options{
		Version:   version,
		ReplicaID: cfg.ReplicaID,
		Logger:    log,
		Repo:      meta.New(pool),
		Blobs:     blobs,
	})

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.GRPC.Serve(lis) }()
	srv.Health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	log.Info("serving", "addr", cfg.GRPCAddr, "version", version)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown: report NOT_SERVING so load balancers stop sending new
	// work, then let in-flight RPCs finish, with a hard deadline.
	log.Info("shutting down")
	srv.Health.Shutdown()
	done := make(chan struct{})
	go func() { srv.GRPC.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		srv.GRPC.Stop()
	}
	if err := <-serveErr; err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}
