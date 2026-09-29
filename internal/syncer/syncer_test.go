package syncer

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dharmikchandel/go-sync/internal/client"
	"github.com/dharmikchandel/go-sync/internal/testenv"
)

// device is one simulated computer: a folder and a Syncer for it.
type device struct {
	t   *testing.T
	dir string
	s   *Syncer
	c   *client.Client
}

func newDevice(t *testing.T, srv *testenv.Server, user, name string) *device {
	t.Helper()
	d := &device{t: t, dir: t.TempDir(), c: srv.Client(t, user)}
	d.open(user, name)
	return d
}

func (d *device) open(user, name string) {
	d.t.Helper()
	s, err := Open(d.dir, d.c, user, Options{Device: name, Logger: testLogger(d.t, name)})
	if err != nil {
		d.t.Fatal(err)
	}
	// A fixed clock gives predictable conflicted-copy names.
	s.now = func() time.Time { return time.Date(2026, 9, 28, 15, 4, 5, 0, time.UTC) }
	d.s = s
	d.t.Cleanup(func() { s.Close() })
}

func (d *device) sync() {
	d.t.Helper()
	if err := d.s.SyncOnce(context.Background()); err != nil {
		d.t.Fatalf("sync %s: %v", d.s.opts.Device, err)
	}
}

func (d *device) write(p, content string) {
	d.t.Helper()
	full := filepath.Join(d.dir, filepath.FromSlash(p))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		d.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		d.t.Fatal(err)
	}
}

func (d *device) remove(p string) {
	d.t.Helper()
	if err := os.Remove(filepath.Join(d.dir, filepath.FromSlash(p))); err != nil {
		d.t.Fatal(err)
	}
}

// files returns every synced file and its content.
func (d *device) files() map[string]string {
	d.t.Helper()
	files := map[string]string{}
	paths, err := d.s.scan()
	if err != nil {
		d.t.Fatal(err)
	}
	for _, p := range paths {
		data, err := os.ReadFile(d.s.abs(p))
		if err != nil {
			d.t.Fatal(err)
		}
		files[p] = string(data)
	}
	return files
}

func (d *device) expect(want map[string]string) {
	d.t.Helper()
	if got := d.files(); !maps.Equal(got, want) {
		d.t.Fatalf("%s has\n  %v\nwant\n  %v", d.s.opts.Device, got, want)
	}
}

// pair returns two devices of one new user, both synced to an empty server.
func pair(t *testing.T) (laptop, phone *device) {
	srv := testenv.StartServer(t)
	user := testenv.User(t)
	return newDevice(t, srv, user, "laptop"), newDevice(t, srv, user, "phone")
}

func TestCreateEditDeletePropagate(t *testing.T) {
	laptop, phone := pair(t)

	laptop.write("notes.txt", "hello")
	laptop.write("docs/deep/plan.md", "plan")
	laptop.write("empty", "")
	laptop.sync()
	phone.sync()
	phone.expect(map[string]string{"notes.txt": "hello", "docs/deep/plan.md": "plan", "empty": ""})

	phone.write("notes.txt", "hello from the phone")
	phone.sync()
	laptop.sync()
	laptop.expect(map[string]string{"notes.txt": "hello from the phone", "docs/deep/plan.md": "plan", "empty": ""})

	laptop.remove("docs/deep/plan.md")
	laptop.sync()
	phone.sync()
	phone.expect(map[string]string{"notes.txt": "hello from the phone", "empty": ""})
	if _, err := os.Stat(filepath.Join(phone.dir, "docs")); !errors.Is(err, fs.ErrNotExist) {
		t.Error("empty directories left behind after a remote delete")
	}

	// Re-creating a deleted file must commit on top of the tombstone.
	phone.write("docs/deep/plan.md", "plan v2")
	phone.sync()
	laptop.sync()
	laptop.expect(phone.files())
}

// The core scenario: both devices edit the same file while offline.
func TestConflictKeepsBothEdits(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("notes.txt", "v1")
	laptop.sync()
	phone.sync()

	laptop.write("notes.txt", "laptop edit")
	phone.write("notes.txt", "phone edit")
	laptop.sync() // laptop reaches the server first and wins the name
	phone.sync()  // phone finds the conflict
	laptop.sync()

	want := map[string]string{
		"notes.txt": "laptop edit",
		"notes (conflicted copy from phone 2026-09-28 150405).txt": "phone edit",
	}
	laptop.expect(want)
	phone.expect(want)
}

func TestEditBeatsDelete(t *testing.T) {
	for _, deleterFirst := range []bool{true, false} {
		laptop, phone := pair(t)
		laptop.write("f", "v1")
		laptop.sync()
		phone.sync()

		laptop.remove("f")
		phone.write("f", "edited")
		if deleterFirst {
			laptop.sync()
			phone.sync()
			laptop.sync()
		} else {
			phone.sync()
			laptop.sync()
			phone.sync()
		}
		laptop.expect(map[string]string{"f": "edited"})
		phone.expect(map[string]string{"f": "edited"})
	}
}

func TestSameEditOnBothIsNotAConflict(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("f", "v1")
	laptop.sync()
	phone.sync()

	laptop.write("f", "same")
	phone.write("f", "same")
	laptop.sync()
	phone.sync()
	phone.expect(map[string]string{"f": "same"})
}

// Two folders that already have files are merged on first sync.
func TestFirstSyncMergesFolders(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("only-laptop", "l")
	laptop.write("both-same", "same")
	laptop.write("both-different", "laptop")
	phone.write("only-phone", "p")
	phone.write("both-same", "same")
	phone.write("both-different", "phone")

	laptop.sync()
	phone.sync()
	laptop.sync()

	want := map[string]string{
		"only-laptop":    "l",
		"only-phone":     "p",
		"both-same":      "same",
		"both-different": "laptop",
		"both-different (conflicted copy from phone 2026-09-28 150405)": "phone",
	}
	laptop.expect(want)
	phone.expect(want)
}

// A crash after the server accepted an upload but before the device
// recorded it. On restart the device must not upload again or report a
// conflict with itself.
func TestCrashAfterUploadBeforeRecord(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("f", "v1")
	laptop.sync()

	before, _ := laptop.s.state.get("f")
	cursor, _ := laptop.s.state.cursor()
	laptop.write("f", "v2")
	laptop.sync() // uploads v2

	// Roll the local record back, as if the process died right after Commit.
	laptop.s.state.put(before)
	laptop.s.state.setCursor(cursor)
	laptop.sync()

	history, err := laptop.c.History(context.Background(), "f")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("server has %d versions, want 2 (no duplicate upload)", len(history))
	}
	after, _ := laptop.s.state.get("f")
	if after.Version != 2 {
		t.Fatalf("record says v%d, want v2", after.Version)
	}
	phone.sync()
	laptop.expect(map[string]string{"f": "v2"})
	phone.expect(map[string]string{"f": "v2"})
}

// The server moves on between our pull and our upload: the commit is
// rejected, and the syncer must re-check and resolve it as a conflict.
func TestServerChangesDuringSync(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("f", "v1")
	laptop.sync()
	phone.sync()

	phone.write("f", "phone edit")
	laptop.write("f", "laptop edit")
	laptop.sync()

	// Reconcile the phone's path as if its pull had not seen the laptop's
	// commit (change == nil): its upload is based on v1 and gets rejected.
	if err := phone.s.syncPath(context.Background(), "f", nil); err != nil {
		t.Fatal(err)
	}
	phone.sync()
	laptop.sync()
	want := map[string]string{
		"f": "laptop edit",
		"f (conflicted copy from phone 2026-09-28 150405)": "phone edit",
	}
	laptop.expect(want)
	phone.expect(want)
}

// A download must not overwrite an edit made while it was in flight.
func TestLocalEditDuringDownloadIsKept(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("f", "v1")
	laptop.sync()
	phone.sync()
	laptop.write("f", "v2")
	laptop.sync()

	// The phone decided to download v2 while its file was "v1". Now the
	// user edits the file before the download finishes.
	info, _ := os.Lstat(phone.s.abs("f"))
	file, err := phone.c.GetFile(context.Background(), "f", 2)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // make sure the mtime moves
	phone.write("f", "typing...")
	if err := phone.s.download(context.Background(), "f", file, info); !errors.Is(err, errLocalChanged) {
		t.Fatalf("got %v, want errLocalChanged", err)
	}
	phone.expect(map[string]string{"f": "typing..."})

	// The next cycle sees both edits and keeps both.
	phone.sync()
	phone.expect(map[string]string{
		"f": "v2",
		"f (conflicted copy from phone 2026-09-28 150405)": "typing...",
	})
}

// A server change that fails to apply must be retried by later cycles: the
// cursor may not move past it.
func TestFailedChangeIsRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	laptop, phone := pair(t)
	laptop.write("docs/a", "a")
	laptop.sync()
	phone.sync()

	laptop.write("docs/b", "b")
	laptop.write("z", "z") // a later change in the feed that does apply
	laptop.sync()

	// The phone can't write into docs/, so downloading docs/b fails.
	docs := filepath.Join(phone.dir, "docs")
	os.Chmod(docs, 0o555)
	defer os.Chmod(docs, 0o755)
	if err := phone.s.SyncOnce(context.Background()); err == nil {
		t.Fatal("sync succeeded with an unwritable directory")
	}
	phone.expect(map[string]string{"docs/a": "a", "z": "z"})

	os.Chmod(docs, 0o755)
	phone.sync()
	phone.expect(map[string]string{"docs/a": "a", "docs/b": "b", "z": "z"})
}

func TestFolderCanOnlyBeOpenedOnce(t *testing.T) {
	srv := testenv.StartServer(t)
	user := testenv.User(t)
	d := newDevice(t, srv, user, "laptop")

	_, err := Open(d.dir, d.c, user, Options{})
	if !errors.Is(err, ErrFolderInUse) {
		t.Fatalf("second open: got %v, want ErrFolderInUse", err)
	}

	d.s.Close()
	_, err = Open(d.dir, d.c, "someone-else", Options{})
	if err == nil || !strings.Contains(err.Error(), "synced as user") {
		t.Fatalf("open as another user: got %v", err)
	}
}

func TestIgnoresStateAndTempFiles(t *testing.T) {
	laptop, phone := pair(t)
	laptop.write("real", "x")
	laptop.write(client.TempPrefix+"123", "partial download")
	laptop.sync()
	phone.sync()
	phone.expect(map[string]string{"real": "x"})
	if _, err := os.Stat(filepath.Join(phone.dir, StateDir, "state.db")); err != nil {
		t.Fatal(err)
	}
}

// The daemon: file events on one device reach the other with no manual
// sync calls.
func TestDaemonsConverge(t *testing.T) {
	laptop, phone := pair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, d := range []*device{laptop, phone} {
		d.s.opts.PollInterval = 100 * time.Millisecond
		go d.s.Run(ctx)
	}

	laptop.write("a/b/c.txt", "from the laptop")
	waitFor(t, phone, map[string]string{"a/b/c.txt": "from the laptop"})

	phone.write("a/b/c.txt", "edited on the phone")
	phone.write("new.txt", "new")
	want := map[string]string{"a/b/c.txt": "edited on the phone", "new.txt": "new"}
	waitFor(t, laptop, want)

	laptop.remove("new.txt")
	waitFor(t, phone, map[string]string{"a/b/c.txt": "edited on the phone"})
}

func waitFor(t *testing.T, d *device, want map[string]string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !maps.Equal(d.files(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached %v; has %v", d.s.opts.Device, slices.Sorted(maps.Keys(want)), d.files())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestConflictNames(t *testing.T) {
	s := &Syncer{root: t.TempDir(), opts: Options{Device: "my/laptop"}, now: func() time.Time {
		return time.Date(2026, 9, 28, 15, 4, 5, 0, time.UTC)
	}}
	for p, want := range map[string]string{
		"notes.txt":    "notes (conflicted copy from my-laptop 2026-09-28 150405).txt",
		"dir/a.tar.gz": "dir/a.tar (conflicted copy from my-laptop 2026-09-28 150405).gz",
		".bashrc":      ".bashrc (conflicted copy from my-laptop 2026-09-28 150405)",
		"Makefile":     "Makefile (conflicted copy from my-laptop 2026-09-28 150405)",
	} {
		got, err := s.freeConflictName(p)
		if err != nil || got != want {
			t.Errorf("%s: got %q, %v; want %q", p, got, err, want)
		}
	}
	// A taken name gets a number.
	os.WriteFile(filepath.Join(s.root, "x (conflicted copy from my-laptop 2026-09-28 150405)"), nil, 0o644)
	if got, _ := s.freeConflictName("x"); got != "x (conflicted copy from my-laptop 2026-09-28 150405 2)" {
		t.Errorf("got %q", got)
	}
}

// testLogger sends a syncer's log to the test output (shown with -v or on
// failure).
func testLogger(t *testing.T, device string) *slog.Logger {
	return slog.New(slog.NewTextHandler(tWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug})).With("device", device)
}

type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
