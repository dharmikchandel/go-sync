package meta_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/dharmikchandel/go-sync/internal/meta"
	"github.com/dharmikchandel/go-sync/internal/testenv"
)

// setup returns a repo and a fresh user, isolated from other tests.
func setup(t *testing.T) (*meta.Repo, int64) {
	t.Helper()
	repo := meta.New(testenv.Pool(t))
	user, err := repo.EnsureUser(context.Background(), testenv.User(t))
	if err != nil {
		t.Fatal(err)
	}
	return repo, user
}

// block stores a block with the given content and returns its hash.
func block(t *testing.T, repo *meta.Repo, user int64, content string) []byte {
	t.Helper()
	h := sha256.Sum256([]byte(content))
	if err := repo.AddBlock(context.Background(), user, h[:], len(content)); err != nil {
		t.Fatal(err)
	}
	return h[:]
}

func commit(t *testing.T, repo *meta.Repo, c meta.Commit) meta.FileVersion {
	t.Helper()
	v, err := repo.Commit(context.Background(), c)
	if err != nil {
		t.Fatalf("commit %s base %d: %v", c.Path, c.BaseVersion, err)
	}
	return v
}

func wantConflict(t *testing.T, err error, current int64) {
	t.Helper()
	var conflict *meta.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("got %v, want ConflictError", err)
	}
	if conflict.Current != current {
		t.Fatalf("conflict reports current v%d, want v%d", conflict.Current, current)
	}
}

// The invariant everything else rests on: of many writers that all start from
// the same version, exactly one wins, and every other one is told it's stale.
// A mock can't prove this; it depends on real Postgres locking.
func TestConcurrentCommitsFromSameBase(t *testing.T) {
	repo, user := setup(t)
	ctx := context.Background()
	h := block(t, repo, user, "hello")
	commit(t, repo, meta.Commit{UserID: user, Path: "a.txt", Blocks: [][]byte{h}})

	const writers = 20
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		wins      int
		conflicts int
	)
	for range writers {
		wg.Go(func() {
			_, err := repo.Commit(ctx, meta.Commit{UserID: user, Path: "a.txt", BaseVersion: 1, Blocks: [][]byte{h}})
			mu.Lock()
			defer mu.Unlock()
			var conflict *meta.ConflictError
			switch {
			case err == nil:
				wins++
			case errors.As(err, &conflict) && conflict.Current == 2:
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	wg.Wait()

	if wins != 1 || conflicts != writers-1 {
		t.Fatalf("got %d wins and %d conflicts, want 1 and %d", wins, conflicts, writers-1)
	}
}

// The lost-update scenario the original prototype got wrong: the laptop edits
// v1 offline, the phone uploads v2, then the laptop comes back. The laptop's
// write must be rejected, not silently stacked on top of the phone's.
func TestStaleWriteIsRejected(t *testing.T) {
	repo, user := setup(t)
	ctx := context.Background()
	v1 := block(t, repo, user, "v1")
	commit(t, repo, meta.Commit{UserID: user, Path: "notes.txt", Blocks: [][]byte{v1}})

	phone := block(t, repo, user, "phone edit")
	commit(t, repo, meta.Commit{UserID: user, Path: "notes.txt", BaseVersion: 1, Blocks: [][]byte{phone}})

	laptop := block(t, repo, user, "laptop edit")
	_, err := repo.Commit(ctx, meta.Commit{UserID: user, Path: "notes.txt", BaseVersion: 1, Blocks: [][]byte{laptop}})
	wantConflict(t, err, 2)

	// Creating a file that already exists is also a stale write.
	_, err = repo.Commit(ctx, meta.Commit{UserID: user, Path: "notes.txt", BaseVersion: 0, Blocks: [][]byte{laptop}})
	wantConflict(t, err, 2)

	// And so is a base from the future (or for a file that doesn't exist).
	_, err = repo.Commit(ctx, meta.Commit{UserID: user, Path: "other.txt", BaseVersion: 3})
	wantConflict(t, err, 0)
}

func TestChangeFeed(t *testing.T) {
	repo, user := setup(t)
	ctx := context.Background()
	h := block(t, repo, user, "x")

	commit(t, repo, meta.Commit{UserID: user, Path: "a", Blocks: [][]byte{h}})                 // seq 1
	commit(t, repo, meta.Commit{UserID: user, Path: "b", Blocks: [][]byte{h}})                 // seq 2
	commit(t, repo, meta.Commit{UserID: user, Path: "a", BaseVersion: 1, Blocks: [][]byte{h}}) // seq 3
	commit(t, repo, meta.Commit{UserID: user, Path: "b", BaseVersion: 1, Deleted: true})       // seq 4
	commit(t, repo, meta.Commit{UserID: user, Path: "c", Blocks: [][]byte{h}})                 // seq 5

	type change struct {
		path    string
		version int64
		seq     int64
		deleted bool
	}
	list := func(since int64, limit int) []change {
		t.Helper()
		vs, err := repo.ListChanges(ctx, user, since, limit)
		if err != nil {
			t.Fatal(err)
		}
		var out []change
		for _, v := range vs {
			out = append(out, change{v.Path, v.Version, v.Seq, v.Deleted})
		}
		return out
	}

	// From the start: each file once, at its latest version, in commit order.
	// "a" moved past "b" because its latest change (seq 3) is newer than b's
	// creation (seq 2). The delete shows up as a tombstone.
	want := []change{{"a", 2, 3, false}, {"b", 2, 4, true}, {"c", 1, 5, false}}
	if got := list(0, 100); !slices.Equal(got, want) {
		t.Fatalf("since 0:\ngot  %v\nwant %v", got, want)
	}
	// A device that last saw seq 3 only gets what came after.
	if got := list(3, 100); !slices.Equal(got, want[1:]) {
		t.Fatalf("since 3: got %v, want %v", got, want[1:])
	}
	// Paging.
	if got := list(0, 2); !slices.Equal(got, want[:2]) {
		t.Fatalf("page 1: got %v, want %v", got, want[:2])
	}
	if got := list(4, 2); !slices.Equal(got, want[2:]) {
		t.Fatalf("page 2: got %v, want %v", got, want[2:])
	}
}

// Seqs must have no gaps and must match commit order even when many commits
// run at once. A gap or reordering is how a device's cursor skips a change.
func TestSeqsAreGapFreeUnderConcurrency(t *testing.T) {
	repo, user := setup(t)
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, err := repo.Commit(ctx, meta.Commit{UserID: user, Path: fmt.Sprintf("f%d", i)}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	changes, err := repo.ListChanges(ctx, user, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != n {
		t.Fatalf("got %d changes, want %d", len(changes), n)
	}
	for i, c := range changes {
		if c.Seq != int64(i+1) {
			t.Fatalf("change %d has seq %d, want %d (gap or reorder)", i, c.Seq, i+1)
		}
	}
}

func TestCommitRequiresStoredBlocks(t *testing.T) {
	repo, user := setup(t)
	ctx := context.Background()
	stored := block(t, repo, user, "stored")
	unknown := sha256.Sum256([]byte("never uploaded"))

	_, err := repo.Commit(ctx, meta.Commit{UserID: user, Path: "f", Blocks: [][]byte{stored, unknown[:]}})
	var missing *meta.MissingBlocksError
	if !errors.As(err, &missing) || len(missing.Hashes) != 1 || !slices.Equal(missing.Hashes[0], unknown[:]) {
		t.Fatalf("got %v, want MissingBlocksError listing only the unknown block", err)
	}

	// Once uploaded it commits, and the size is computed from stored block
	// sizes, counting a block that appears twice twice.
	block(t, repo, user, "never uploaded")
	v := commit(t, repo, meta.Commit{UserID: user, Path: "f", Blocks: [][]byte{stored, unknown[:], stored}})
	if want := int64(len("stored")*2 + len("never uploaded")); v.Size != want {
		t.Fatalf("size = %d, want %d", v.Size, want)
	}

	got, err := repo.GetFile(ctx, user, "f", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Blocks) != 3 || !slices.Equal(got.Blocks[1].Hash, unknown[:]) {
		t.Fatalf("manifest not stored in order: %+v", got.Blocks)
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	repo, user := setup(t)
	ctx := context.Background()
	h := block(t, repo, user, "x")

	if _, err := repo.Commit(ctx, meta.Commit{UserID: user, Path: "f", Deleted: true}); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("deleting a missing file: got %v, want ErrNotFound", err)
	}

	commit(t, repo, meta.Commit{UserID: user, Path: "f", Blocks: [][]byte{h}})
	tomb := commit(t, repo, meta.Commit{UserID: user, Path: "f", BaseVersion: 1, Deleted: true})
	if !tomb.Deleted || tomb.Version != 2 || tomb.Size != 0 {
		t.Fatalf("tombstone = %+v", tomb)
	}
	if _, err := repo.Commit(ctx, meta.Commit{UserID: user, Path: "f", BaseVersion: 2, Deleted: true}); !errors.Is(err, meta.ErrAlreadyDeleted) {
		t.Fatalf("deleting twice: got %v, want ErrAlreadyDeleted", err)
	}

	// Recreating builds on the tombstone, so history stays in one line.
	v := commit(t, repo, meta.Commit{UserID: user, Path: "f", BaseVersion: 2, Blocks: [][]byte{h}})
	if v.Version != 3 {
		t.Fatalf("recreated as v%d, want v3", v.Version)
	}
	history, err := repo.ListVersions(ctx, user, "f")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 || history[0].Version != 3 || !history[1].Deleted {
		t.Fatalf("history = %+v", history)
	}
}

func TestUsersAreIsolated(t *testing.T) {
	repo, alice := setup(t)
	_, bob := setup(t)
	ctx := context.Background()

	h := block(t, repo, alice, "alice's secret")
	commit(t, repo, meta.Commit{UserID: alice, Path: "secret.txt", Blocks: [][]byte{h}})

	if _, err := repo.GetFile(ctx, bob, "secret.txt", 0); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("bob reading alice's file: got %v, want ErrNotFound", err)
	}
	if changes, _ := repo.ListChanges(ctx, bob, 0, 10); len(changes) != 0 {
		t.Fatalf("bob sees alice's changes: %+v", changes)
	}
	// Knowing a block's hash must not let bob use alice's block.
	var missing *meta.MissingBlocksError
	if _, err := repo.Commit(ctx, meta.Commit{UserID: bob, Path: "stolen", Blocks: [][]byte{h}}); !errors.As(err, &missing) {
		t.Fatalf("bob committing alice's block: got %v, want MissingBlocksError", err)
	}
}
