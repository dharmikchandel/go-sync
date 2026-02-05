package main

import (
	"log"
	"net"
	"context"
	"os"

	"google.golang.org/grpc"

	pb "go-sync/proto"
	"go-sync/internal/metadata"
	"go-sync/internal/storage"
	"go-sync/internal/upload"
	"go-sync/internal/lock"
	"go-sync/internal/notify"
	"go-sync/internal/gc"
	"go-sync/internal/events"

	"google.golang.org/grpc/reflection"
)

func main() {
	POSTGRES_URL := os.Getenv("POSTGRES_URL")
	REDIS_ADDR := os.Getenv("REDIS_ADDR")
	MINIO_ENDPOINT := os.Getenv("MINIO_ENDPOINT")
	NATS_URL := os.Getenv("NATS_URL")

	// postgres
	repo, err := metadata.NewRepository(
		POSTGRES_URL,
	)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer repo.Close()

	log.Println("Connected to PostgreSQL")

	// minio
	store, err := storage.New(
		MINIO_ENDPOINT, // MinIO endpoint
		"minio",          // access key
		"minio123",       // secret key
		"go-sync-blobs",   // bucket name
	)
	if err != nil {
		log.Fatalf("failed to connect minio: %v", err)
	}

	log.Println("Connected to MinIO")

	// gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// reflection API support
	reflection.Register(grpcServer)

	// redis lock
	locker := lock.New(REDIS_ADDR)

	// nats
	bus, err := events.New(NATS_URL)
	if err != nil {
		log.Fatal(err)
	}

	hub := notify.NewHub()

	bus.Subscribe(func(event *pb.FileEvent) {
		hub.Dispatch(event)
	})

	handler := upload.NewServer(repo, store, locker, hub, bus)

	ctx := context.Background()
	worker := gc.NewWorker(repo, store)
	worker.Start(ctx)

	pb.RegisterFileSyncServiceServer(grpcServer, handler)

	log.Println("GoSync gRPC server running on :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc serve failed: %v", err)
	}
}