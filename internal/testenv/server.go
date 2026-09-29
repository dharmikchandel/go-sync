package testenv

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dharmikchandel/go-sync/internal/blob"
	"github.com/dharmikchandel/go-sync/internal/client"
	"github.com/dharmikchandel/go-sync/internal/meta"
	"github.com/dharmikchandel/go-sync/internal/server"
)

// Server is the real go-sync server (real Postgres, real S3) listening on
// an in-memory connection: the whole gRPC stack runs, but no port is bound.
type Server struct {
	Repo  *meta.Repo
	Blobs *blob.Store
	lis   *bufconn.Listener
}

func StartServer(t *testing.T) *Server {
	t.Helper()
	repo := meta.New(Pool(t))
	s3 := SharedObjectStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blobs, err := blob.Open(ctx, blob.Options{Endpoint: s3.Endpoint, AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Bucket: "gosync-test"})
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(server.Options{
		Version:   "test",
		ReplicaID: "test",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Repo:      repo,
		Blobs:     blobs,
	})
	lis := bufconn.Listen(1 << 20)
	go srv.GRPC.Serve(lis)
	t.Cleanup(srv.GRPC.Stop)
	return &Server{Repo: repo, Blobs: blobs, lis: lis}
}

// DialOption routes a gRPC client to this server. Use it with the target
// "passthrough:///bufnet".
func (s *Server) DialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return s.lis.DialContext(ctx) })
}

// Client connects as user.
func (s *Server) Client(t *testing.T, user string) *client.Client {
	t.Helper()
	c, err := client.Dial("passthrough:///bufnet", user, s.DialOption())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
