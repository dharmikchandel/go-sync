package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/blob"
	"github.com/dharmikchandel/go-sync/internal/chunk"
	"github.com/dharmikchandel/go-sync/internal/meta"
	"github.com/dharmikchandel/go-sync/internal/syncpath"
)

const (
	// maxManifestBlocks caps a file at ~400 GiB of 4 MiB blocks.
	maxManifestBlocks = 100_000
	maxChangesPage    = 1000
)

func blockKey(userID int64, hash []byte) string {
	return fmt.Sprintf("blocks/%d/%s", userID, hex.EncodeToString(hash))
}

func (s *syncService) PutBlock(ctx context.Context, req *gosyncv1.PutBlockRequest) (*gosyncv1.PutBlockResponse, error) {
	if len(req.Data) > chunk.BlockSize {
		return nil, status.Errorf(codes.InvalidArgument, "block is larger than %d bytes", chunk.BlockSize)
	}
	// Never trust a client-supplied content hash: a wrong one would make
	// every later download of this block fail its integrity check.
	sum := sha256.Sum256(req.Data)
	if !bytes.Equal(sum[:], req.Hash) {
		return nil, status.Error(codes.InvalidArgument, "hash does not match block data")
	}

	user := userID(ctx)
	exists, err := s.repo.HasBlock(ctx, user, req.Hash)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	if exists {
		return &gosyncv1.PutBlockResponse{AlreadyExisted: true}, nil
	}

	// Bytes first, then metadata. If we crash in between, the object is
	// an unreferenced orphan (wasted space, cleaned up by GC later). The
	// other order would leave a blocks row pointing at nothing, which
	// breaks every file that uses it.
	if err := s.blobs.Put(ctx, blockKey(user, req.Hash), req.Data); err != nil {
		return nil, s.toStatus(ctx, err)
	}
	if err := s.repo.AddBlock(ctx, user, req.Hash, len(req.Data)); err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &gosyncv1.PutBlockResponse{}, nil
}

func (s *syncService) GetBlock(ctx context.Context, req *gosyncv1.GetBlockRequest) (*gosyncv1.GetBlockResponse, error) {
	user := userID(ctx)
	// The blocks table is the authority on ownership: only fetch blocks this
	// user has stored.
	exists, err := s.repo.HasBlock(ctx, user, req.Hash)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	if !exists {
		return nil, status.Error(codes.NotFound, "block not found")
	}
	data, err := s.blobs.Get(ctx, blockKey(user, req.Hash))
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &gosyncv1.GetBlockResponse{Data: data}, nil
}

func (s *syncService) CommitFile(ctx context.Context, req *gosyncv1.CommitFileRequest) (*gosyncv1.CommitFileResponse, error) {
	if err := validateWrite(req.Path, req.BaseVersion); err != nil {
		return nil, err
	}
	if len(req.BlockHashes) > maxManifestBlocks {
		return nil, status.Errorf(codes.InvalidArgument, "file has more than %d blocks", maxManifestBlocks)
	}
	for _, h := range req.BlockHashes {
		if len(h) != sha256.Size {
			return nil, status.Error(codes.InvalidArgument, "block hash must be 32 bytes")
		}
	}

	v, err := s.repo.Commit(ctx, meta.Commit{
		UserID:      userID(ctx),
		Path:        req.Path,
		BaseVersion: req.BaseVersion,
		Blocks:      req.BlockHashes,
	})
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &gosyncv1.CommitFileResponse{File: toProto(v)}, nil
}

func (s *syncService) DeleteFile(ctx context.Context, req *gosyncv1.DeleteFileRequest) (*gosyncv1.DeleteFileResponse, error) {
	if err := validateWrite(req.Path, req.BaseVersion); err != nil {
		return nil, err
	}
	v, err := s.repo.Commit(ctx, meta.Commit{
		UserID:      userID(ctx),
		Path:        req.Path,
		BaseVersion: req.BaseVersion,
		Deleted:     true,
	})
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &gosyncv1.DeleteFileResponse{File: toProto(v)}, nil
}

func (s *syncService) GetFile(ctx context.Context, req *gosyncv1.GetFileRequest) (*gosyncv1.GetFileResponse, error) {
	if err := syncpath.Validate(req.Path); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.Version < 0 {
		return nil, status.Error(codes.InvalidArgument, "version must be >= 0")
	}
	v, err := s.repo.GetFile(ctx, userID(ctx), req.Path, req.Version)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &gosyncv1.GetFileResponse{File: toProto(v)}, nil
}

func (s *syncService) ListFileVersions(ctx context.Context, req *gosyncv1.ListFileVersionsRequest) (*gosyncv1.ListFileVersionsResponse, error) {
	if err := syncpath.Validate(req.Path); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	versions, err := s.repo.ListVersions(ctx, userID(ctx), req.Path)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	resp := &gosyncv1.ListFileVersionsResponse{}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, toProto(v))
	}
	return resp, nil
}

func (s *syncService) ListChanges(ctx context.Context, req *gosyncv1.ListChangesRequest) (*gosyncv1.ListChangesResponse, error) {
	if req.Since < 0 {
		return nil, status.Error(codes.InvalidArgument, "since must be >= 0")
	}
	limit := int(req.Limit)
	if limit <= 0 || limit > maxChangesPage {
		limit = maxChangesPage
	}

	// Fetch one extra row to learn whether another page exists.
	changes, err := s.repo.ListChanges(ctx, userID(ctx), req.Since, limit+1)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	resp := &gosyncv1.ListChangesResponse{NextSince: req.Since}
	if len(changes) > limit {
		resp.HasMore = true
		changes = changes[:limit]
	}
	for _, v := range changes {
		resp.Changes = append(resp.Changes, toProto(v))
		resp.NextSince = v.Seq
	}
	return resp, nil
}

func validateWrite(path string, baseVersion int64) error {
	if err := syncpath.Validate(path); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if baseVersion < 0 {
		return status.Error(codes.InvalidArgument, "base_version must be >= 0")
	}
	return nil
}

// toStatus maps domain errors to gRPC status codes. Anything unexpected
// becomes INTERNAL with a generic message: the details go to the log, not to
// the client.
func (s *syncService) toStatus(ctx context.Context, err error) error {
	var (
		conflict *meta.ConflictError
		missing  *meta.MissingBlocksError
	)
	switch {
	case errors.As(err, &conflict):
		st, _ := status.New(codes.Aborted, err.Error()).
			WithDetails(&gosyncv1.VersionConflict{CurrentVersion: conflict.Current})
		return st.Err()
	case errors.As(err, &missing):
		st, _ := status.New(codes.FailedPrecondition, err.Error()).
			WithDetails(&gosyncv1.MissingBlocks{Hashes: missing.Hashes})
		return st.Err()
	case errors.Is(err, meta.ErrNotFound), errors.Is(err, blob.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, meta.ErrAlreadyDeleted):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	s.log.ErrorContext(ctx, "internal error", "err", err)
	return status.Error(codes.Internal, "internal error")
}

func toProto(v meta.FileVersion) *gosyncv1.FileVersion {
	p := &gosyncv1.FileVersion{
		Path:      v.Path,
		Version:   v.Version,
		Seq:       v.Seq,
		Deleted:   v.Deleted,
		Size:      v.Size,
		CreatedAt: timestamppb.New(v.CreatedAt),
	}
	for _, b := range v.Blocks {
		p.Blocks = append(p.Blocks, &gosyncv1.BlockRef{Hash: b.Hash, Size: b.Size})
	}
	return p
}
