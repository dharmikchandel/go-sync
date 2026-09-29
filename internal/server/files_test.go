package server_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/chunk"
	"github.com/dharmikchandel/go-sync/internal/client"
	"github.com/dharmikchandel/go-sync/internal/testenv"
)

func writeRandomFile(t *testing.T, path string, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	rand.Read(data)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return data
}

// Upload then download must give back identical bytes at every size that
// touches a block boundary. The original prototype returned empty files for
// anything under 64 KiB; this is the test that would have caught it.
func TestRoundTripAtBlockBoundaries(t *testing.T) {
	e := testenv.StartServer(t)
	c := e.Client(t, testenv.User(t))
	ctx := context.Background()
	dir := t.TempDir()

	for _, size := range []int{0, 1, chunk.BlockSize - 1, chunk.BlockSize, chunk.BlockSize + 1, 2*chunk.BlockSize + 123} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			src := filepath.Join(dir, fmt.Sprintf("src-%d", size))
			dst := filepath.Join(dir, fmt.Sprintf("dst-%d", size))
			want := writeRandomFile(t, src, size)
			remote := fmt.Sprintf("files/%d.bin", size)

			up, err := c.Upload(ctx, src, remote, 0)
			if err != nil {
				t.Fatal(err)
			}
			if up.Size != int64(size) {
				t.Fatalf("server recorded size %d, want %d", up.Size, size)
			}
			if _, err := c.Download(ctx, remote, 0, dst); err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(dst)
			if !bytes.Equal(got, want) {
				t.Fatalf("downloaded %d bytes that differ from the %d uploaded", len(got), len(want))
			}
		})
	}
}

// A failed download must never damage the local file. The prototype
// truncated it to zero bytes before even contacting the server.
func TestFailedDownloadLeavesLocalFileUntouched(t *testing.T) {
	e := testenv.StartServer(t)
	c := e.Client(t, testenv.User(t))
	dir := t.TempDir()
	local := filepath.Join(dir, "precious.txt")
	os.WriteFile(local, []byte("do not lose me"), 0o644)

	_, err := c.Download(context.Background(), "does/not/exist.txt", 0, local)
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	assertFile(t, local, "do not lose me")
	assertNoTempFiles(t, dir)
}

// If stored bytes are corrupted, the client must detect it from the block
// hash and refuse to write them.
func TestCorruptedBlockIsDetected(t *testing.T) {
	e := testenv.StartServer(t)
	user := testenv.User(t)
	c := e.Client(t, user)
	ctx := context.Background()
	dir := t.TempDir()

	src := filepath.Join(dir, "src")
	writeRandomFile(t, src, 1000)
	v, err := c.Upload(ctx, src, "f.bin", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the stored object behind the server's back.
	full, err := c.GetFile(ctx, "f.bin", v.Version)
	if err != nil {
		t.Fatal(err)
	}
	userID, _ := e.Repo.EnsureUser(ctx, user)
	key := fmt.Sprintf("blocks/%d/%s", userID, hex.EncodeToString(full.Blocks[0].Hash))
	if err := e.Blobs.Put(ctx, key, []byte("bit rot")); err != nil {
		t.Fatal(err)
	}

	local := filepath.Join(dir, "local")
	os.WriteFile(local, []byte("old"), 0o644)
	if _, err := c.Download(ctx, "f.bin", 0, local); err == nil {
		t.Fatal("download of a corrupted block succeeded")
	}
	assertFile(t, local, "old")
	assertNoTempFiles(t, dir)
}

func TestConcurrentEditIsRejected(t *testing.T) {
	e := testenv.StartServer(t)
	user := testenv.User(t)
	laptop, phone := e.Client(t, user), e.Client(t, user)
	ctx := context.Background()
	dir := t.TempDir()
	f := filepath.Join(dir, "f")
	os.WriteFile(f, []byte("v1"), 0o644)

	if _, err := laptop.Upload(ctx, f, "notes.txt", 0); err != nil {
		t.Fatal(err)
	}
	// Both devices now hold v1. The phone uploads first.
	os.WriteFile(f, []byte("phone"), 0o644)
	if _, err := phone.Upload(ctx, f, "notes.txt", 1); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(f, []byte("laptop"), 0o644)
	_, err := laptop.Upload(ctx, f, "notes.txt", 1)

	var conflict *client.ConflictError
	if !errors.As(err, &conflict) || conflict.Current != 2 {
		t.Fatalf("got %v, want ConflictError with current v2", err)
	}
}

func TestRestoreCommitsOldManifest(t *testing.T) {
	e := testenv.StartServer(t)
	c := e.Client(t, testenv.User(t))
	ctx := context.Background()
	dir := t.TempDir()
	f := filepath.Join(dir, "f")

	original := writeRandomFile(t, f, 5000)
	c.Upload(ctx, f, "doc", 0)
	writeRandomFile(t, f, 7000)
	c.Upload(ctx, f, "doc", 1)

	v, err := c.Restore(ctx, "doc", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if v.Version != 3 {
		t.Fatalf("restored as v%d, want v3", v.Version)
	}
	out := filepath.Join(dir, "out")
	if _, err := c.Download(ctx, "doc", 0, out); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, original) {
		t.Fatal("current version after restore doesn't match v1")
	}
}

func TestPutBlockVerifiesHash(t *testing.T) {
	e := testenv.StartServer(t)
	c := e.Client(t, testenv.User(t))
	wrong := sha256.Sum256([]byte("something else"))

	_, err := c.RPC().PutBlock(context.Background(), &gosyncv1.PutBlockRequest{Hash: wrong[:], Data: []byte("data")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestRequestValidation(t *testing.T) {
	e := testenv.StartServer(t)
	c := e.Client(t, testenv.User(t))
	ctx := context.Background()

	for _, p := range []string{"", "/etc/passwd", "../escape", "a//b"} {
		_, err := c.RPC().CommitFile(ctx, &gosyncv1.CommitFileRequest{Path: p})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("path %q: got %v, want InvalidArgument", p, err)
		}
	}

	// No user metadata at all.
	conn, err := grpc.NewClient("passthrough:///bufnet", e.DialOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = gosyncv1.NewSyncServiceClient(conn).ListChanges(ctx, &gosyncv1.ListChangesRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no user: got %v, want Unauthenticated", err)
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", filepath.Base(path), got, want)
	}
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	leftovers, _ := filepath.Glob(filepath.Join(dir, client.TempPrefix+"*"))
	if len(leftovers) > 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}
}
