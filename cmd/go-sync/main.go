// Command go-sync is the go-sync client.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
)

const usage = `usage: go-sync [-server addr] <command>

commands:
  info    show which server replica answered and its version
  health  exit 0 if the server reports SERVING (used by container healthchecks)
`

func main() {
	server := flag.String("server", envOr("GOSYNC_SERVER", "localhost:50051"), "server address")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	var err error
	switch flag.Arg(0) {
	case "info":
		err = info(*server)
	case "health":
		err = checkHealth(*server)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func info(addr string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := gosyncv1.NewSyncServiceClient(conn).GetServerInfo(ctx, &gosyncv1.GetServerInfoRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("server %s (replica %s)\n", resp.Version, resp.ReplicaId)
	return nil
}

func checkHealth(addr string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server is %s", resp.Status)
	}
	fmt.Println("SERVING")
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
