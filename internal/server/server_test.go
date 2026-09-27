package server_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/server"
)

// dial starts the server on an in-memory listener: the full gRPC stack
// (encoding, interceptors, status codes) runs, but no port is bound.
func dial(t *testing.T) (*server.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := server.New(server.Options{
		Version:   "test",
		ReplicaID: "replica-1",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go srv.GRPC.Serve(lis)
	t.Cleanup(srv.GRPC.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return srv, conn
}

func TestGetServerInfo(t *testing.T) {
	_, conn := dial(t)

	resp, err := gosyncv1.NewSyncServiceClient(conn).GetServerInfo(context.Background(), &gosyncv1.GetServerInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Version != "test" || resp.ReplicaId != "replica-1" {
		t.Fatalf("got version=%q replica=%q", resp.Version, resp.ReplicaId)
	}
}

// The server must not report healthy until the caller says startup finished,
// otherwise a load balancer would route traffic to a half-initialised replica.
func TestHealthStartsNotServing(t *testing.T) {
	srv, conn := dial(t)
	health := healthpb.NewHealthClient(conn)

	check := func() healthpb.HealthCheckResponse_ServingStatus {
		resp, err := health.Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Status
	}

	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("before ready: got %v", got)
	}
	srv.Health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	if got := check(); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("after ready: got %v", got)
	}
}
