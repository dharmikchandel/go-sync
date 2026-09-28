// Package server implements the go-sync gRPC API.
package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/api"
	"github.com/dharmikchandel/go-sync/internal/blob"
	"github.com/dharmikchandel/go-sync/internal/meta"
)

type Options struct {
	Version   string
	ReplicaID string
	Logger    *slog.Logger
	Repo      *meta.Repo
	Blobs     *blob.Store
}

// Server wraps the gRPC server together with its health reporting, so the
// caller can flip readiness during startup and shutdown.
type Server struct {
	GRPC   *grpc.Server
	Health *health.Server
}

func New(o Options) *Server {
	g := grpc.NewServer(
		grpc.MaxRecvMsgSize(api.MaxMessageSize),
		grpc.ChainUnaryInterceptor(unaryLogger(o.Logger), authenticate(o.Repo)),
		grpc.ChainStreamInterceptor(streamLogger(o.Logger)),
	)

	gosyncv1.RegisterSyncServiceServer(g, &syncService{
		version:   o.Version,
		replicaID: o.ReplicaID,
		log:       o.Logger,
		repo:      o.Repo,
		blobs:     o.Blobs,
	})

	// Standard gRPC health protocol, so load balancers and probes can check us
	// without knowing our API. Starts NOT_SERVING until the caller is ready.
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(g, h)

	// Reflection lets tools like grpcurl discover the API without .proto files.
	reflection.Register(g)

	return &Server{GRPC: g, Health: h}
}

type syncService struct {
	gosyncv1.UnimplementedSyncServiceServer
	version   string
	replicaID string
	log       *slog.Logger
	repo      *meta.Repo
	blobs     *blob.Store
}

func (s *syncService) GetServerInfo(context.Context, *gosyncv1.GetServerInfoRequest) (*gosyncv1.GetServerInfoResponse, error) {
	return &gosyncv1.GetServerInfoResponse{
		Version:   s.version,
		ReplicaId: s.replicaID,
	}, nil
}

func unaryLogger(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logRPC(log, info.FullMethod, start, err)
		return resp, err
	}
}

func streamLogger(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logRPC(log, info.FullMethod, start, err)
		return err
	}
}

func logRPC(log *slog.Logger, method string, start time.Time, err error) {
	// Health checks arrive every few seconds; logging them drowns everything else.
	if method == healthpb.Health_Check_FullMethodName {
		return
	}
	log.Info("rpc",
		"method", method,
		"code", status.Code(err).String(),
		"duration_ms", time.Since(start).Milliseconds(),
	)
}
