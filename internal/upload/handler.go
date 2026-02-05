package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	// "time"

	"go-sync/internal/events"
	"go-sync/internal/lock"
	"go-sync/internal/metadata"
	"go-sync/internal/notify"
	"go-sync/internal/storage"
	pb "go-sync/proto"
)

type Server struct {
	pb.UnimplementedFileSyncServiceServer
	Repo   *metadata.Repository
	Store  *storage.S3Store
	Locker *lock.Locker

	Hub *notify.Hub
	Bus *events.Bus
}

func NewServer(repo *metadata.Repository, store *storage.S3Store, locker *lock.Locker, hub *notify.Hub, bus *events.Bus) *Server {
	return &Server{
		Repo: repo,
		Store: store,
		Locker: locker,
		Hub: hub,
		Bus: bus,
	}
}

func (s *Server) UploadFile(stream pb.FileSyncService_UploadFileServer) error {
	ctx := context.Background()

	var (
		filename string
		size     int64
		hasher   = sha256.New()
	)

	// Temp file (production safe pattern)
	tmp, err := os.CreateTemp("", "gosync-upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if filename == "" {
			filename = chunk.FileName
		}

		n, err := tmp.Write(chunk.Data)
		if err != nil {
			return err
		}

		hasher.Write(chunk.Data)
		size += int64(n)
	}

	// rewind file for upload
	if _, err := tmp.Seek(0, 0); err != nil {
		return err
	}

	checksum := hex.EncodeToString(hasher.Sum(nil))

	fileID, current, err := s.Repo.GetOrCreateFile(ctx, "user1", filename)
	if err != nil {
		return err
	}

	newVersion := current + 1
	objectKey := fileID + "/" + checksum

	// Upload once with known size (no deadlock)
	if err := s.Store.Upload(ctx, objectKey, tmp, size); err != nil {
		return err
	}

	committed, err := s.Repo.InsertVersionSafe(
		ctx,
		fileID,
		current,
		newVersion,
		objectKey,
		checksum,
		size,
	)
	if err != nil {
		return err
	}

	if !committed {
		return stream.SendAndClose(&pb.UploadStatus{
			Success: false,
			Message: "version conflict, retry",
		})
	}

	s.Bus.Publish(&pb.FileEvent{
		FileId:     fileID,
		Filename:  filename,
		Version:   int32(newVersion),
		Checksum:  checksum,
		SizeBytes: size,
		EventType: "UPDATED",
	})

	return stream.SendAndClose(&pb.UploadStatus{
		Success: true,
		Message: "upload complete",
	})
}

func (s *Server) SyncEvents(
	req *pb.SyncRequest,
	stream pb.FileSyncService_SyncEventsServer,
) error {

	user := req.UserId

	ch := s.Hub.Register(user)
	defer s.Hub.Unregister(user)

	for event := range ch {
		if err := stream.Send(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) DownloadFile(
	req *pb.DownloadRequest,
	stream pb.FileSyncService_DownloadFileServer,
) error {

	ctx := context.Background()

	_, objectKey, err := s.Repo.GetLatestVersion(ctx, req.Filename)
	if err != nil {
		return err
	}

	fmt.Println("DOWNLOADING OBJECT:", objectKey)

	reader, err := s.Store.Download(ctx, objectKey)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, 64*1024)

	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = stream.Send(&pb.DownloadChunk{
			Data: buf[:n],
		})
		if err != nil {
			return err
		}
	}

	return nil
}