// Package client talks to a go-sync server: uploading and downloading files
// as blocks, and reading history and the change feed. The CLI uses it now,
// and the sync daemon will too.
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/api"
	"github.com/dharmikchandel/go-sync/internal/chunk"
)

// parallelism is how many blocks are in flight at once per transfer. It
// bounds memory to about parallelism × 4 MiB while keeping the network busy.
const parallelism = 4

var ErrNotFound = errors.New("file not found")

// ConflictError means the base version given for a write is stale.
type ConflictError struct {
	Path    string
	Base    int64
	Current int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s: version conflict: your base is v%d but the server has v%d", e.Path, e.Base, e.Current)
}

type Client struct {
	conn *grpc.ClientConn
	rpc  gosyncv1.SyncServiceClient
}

// Dial connects to addr, sending user on every request.
func Dial(addr, user string, opts ...grpc.DialOption) (*Client, error) {
	opts = append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(api.MaxMessageSize),
			grpc.MaxCallSendMsgSize(api.MaxMessageSize),
		),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, api.UserMetadataKey, user)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	}, opts...)

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: gosyncv1.NewSyncServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// RPC exposes the raw API for calls this package doesn't wrap.
func (c *Client) RPC() gosyncv1.SyncServiceClient {
	return c.rpc
}

// Upload stores the local file as the next version of remotePath.
// base must be the version the local content was based on (0 for a new
// file); if the server has moved on, it returns *ConflictError.
func (c *Client) Upload(ctx context.Context, localPath, remotePath string, base int64) (*gosyncv1.FileVersion, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	hashes, err := c.putBlocks(ctx, f)
	if err != nil {
		return nil, err
	}

	resp, err := c.rpc.CommitFile(ctx, &gosyncv1.CommitFileRequest{
		Path:        remotePath,
		BaseVersion: base,
		BlockHashes: hashes,
	})
	if err != nil {
		return nil, convertError(err, remotePath, base)
	}
	return resp.File, nil
}

// putBlocks splits r into blocks and uploads them, a few at a time, returning
// the manifest in file order.
func (c *Client) putBlocks(ctx context.Context, r io.Reader) ([][]byte, error) {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism) // g.Go blocks while `parallelism` uploads are running

	var (
		hashes  [][]byte
		readErr error
		chunks  = chunk.NewReader(r)
	)
	// Stop reading early if an upload failed (errgroup cancels ctx).
	for ctx.Err() == nil {
		b, err := chunks.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			readErr = err
			break
		}
		hashes = append(hashes, b.Hash[:])
		g.Go(func() error {
			_, err := c.rpc.PutBlock(ctx, &gosyncv1.PutBlockRequest{Hash: b.Hash[:], Data: b.Data})
			return err
		})
	}
	// Always wait, even after a read error, so no upload goroutine outlives
	// this call.
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, readErr
	}
	return hashes, nil
}

// Download writes a version of remotePath (0 = current) to localPath.
//
// The file is assembled in a temp file next to localPath, every block is
// checked against its hash, and only then is it renamed over localPath. A
// failed or interrupted download leaves the existing local file untouched.
func (c *Client) Download(ctx context.Context, remotePath string, version int64, localPath string) (*gosyncv1.FileVersion, error) {
	file, err := c.GetFile(ctx, remotePath, version)
	if err != nil {
		return nil, err
	}
	if file.Deleted {
		return nil, fmt.Errorf("%s v%d is a deleted version", remotePath, file.Version)
	}

	tmp, err := os.CreateTemp(filepath.Dir(localPath), ".gosync-download-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name()) // fails harmlessly once renamed
	defer tmp.Close()

	if err := c.getBlocks(ctx, file.Blocks, tmp); err != nil {
		return nil, err
	}
	// fsync before rename: otherwise a power loss right after the rename
	// could leave localPath pointing at a file whose data never hit disk.
	if err := tmp.Sync(); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	// rename is atomic: readers see either the old file or the new one.
	if err := os.Rename(tmp.Name(), localPath); err != nil {
		return nil, err
	}
	return file, nil
}

// getBlocks fetches blocks in parallel, each written at its own offset.
func (c *Client) getBlocks(ctx context.Context, blocks []*gosyncv1.BlockRef, f *os.File) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)

	var offset int64
	for _, b := range blocks {
		at := offset
		offset += int64(b.Size)
		g.Go(func() error {
			resp, err := c.rpc.GetBlock(ctx, &gosyncv1.GetBlockRequest{Hash: b.Hash})
			if err != nil {
				return err
			}
			sum := sha256.Sum256(resp.Data)
			if !bytes.Equal(sum[:], b.Hash) || len(resp.Data) != int(b.Size) {
				return fmt.Errorf("block %x failed its integrity check", b.Hash[:8])
			}
			// WriteAt is safe to call concurrently for different ranges.
			_, err = f.WriteAt(resp.Data, at)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	// Set the length explicitly so an empty file (no blocks) is still correct.
	return f.Truncate(offset)
}

// GetFile returns a version of remotePath with its manifest (0 = current).
func (c *Client) GetFile(ctx context.Context, remotePath string, version int64) (*gosyncv1.FileVersion, error) {
	resp, err := c.rpc.GetFile(ctx, &gosyncv1.GetFileRequest{Path: remotePath, Version: version})
	if err != nil {
		return nil, convertError(err, remotePath, 0)
	}
	return resp.File, nil
}

// CurrentVersion returns remotePath's current version number, or 0 if the
// file has never existed.
func (c *Client) CurrentVersion(ctx context.Context, remotePath string) (int64, error) {
	f, err := c.GetFile(ctx, remotePath, 0)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return f.Version, nil
}

// Delete records a tombstone for remotePath.
func (c *Client) Delete(ctx context.Context, remotePath string, base int64) (*gosyncv1.FileVersion, error) {
	resp, err := c.rpc.DeleteFile(ctx, &gosyncv1.DeleteFileRequest{Path: remotePath, BaseVersion: base})
	if err != nil {
		return nil, convertError(err, remotePath, base)
	}
	return resp.File, nil
}

// Restore makes an old version current again by committing its manifest as a
// new version. The blocks are already on the server, so no file data moves.
func (c *Client) Restore(ctx context.Context, remotePath string, version, base int64) (*gosyncv1.FileVersion, error) {
	old, err := c.GetFile(ctx, remotePath, version)
	if err != nil {
		return nil, err
	}
	if old.Deleted {
		return nil, fmt.Errorf("%s v%d is a deleted version", remotePath, version)
	}
	hashes := make([][]byte, len(old.Blocks))
	for i, b := range old.Blocks {
		hashes[i] = b.Hash
	}
	resp, err := c.rpc.CommitFile(ctx, &gosyncv1.CommitFileRequest{
		Path:        remotePath,
		BaseVersion: base,
		BlockHashes: hashes,
	})
	if err != nil {
		return nil, convertError(err, remotePath, base)
	}
	return resp.File, nil
}

// History returns remotePath's versions, newest first.
func (c *Client) History(ctx context.Context, remotePath string) ([]*gosyncv1.FileVersion, error) {
	resp, err := c.rpc.ListFileVersions(ctx, &gosyncv1.ListFileVersionsRequest{Path: remotePath})
	if err != nil {
		return nil, convertError(err, remotePath, 0)
	}
	return resp.Versions, nil
}

// Changes returns every change after since, following pages until caught
// up, and the cursor to pass next time.
func (c *Client) Changes(ctx context.Context, since int64) ([]*gosyncv1.FileVersion, int64, error) {
	var all []*gosyncv1.FileVersion
	for {
		resp, err := c.rpc.ListChanges(ctx, &gosyncv1.ListChangesRequest{Since: since})
		if err != nil {
			return nil, since, err
		}
		all = append(all, resp.Changes...)
		since = resp.NextSince
		if !resp.HasMore {
			return all, since, nil
		}
	}
}

// convertError turns gRPC statuses the caller can act on into typed errors.
func convertError(err error, path string, base int64) error {
	st := status.Convert(err)
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("%s: %w", path, ErrNotFound)
	case codes.Aborted:
		for _, d := range st.Details() {
			if vc, ok := d.(*gosyncv1.VersionConflict); ok {
				return &ConflictError{Path: path, Base: base, Current: vc.CurrentVersion}
			}
		}
	}
	return err
}
