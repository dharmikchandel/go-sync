// Package syncer keeps a local folder in sync with a go-sync server.
//
// One sync cycle:
//  1. Pull: ask the server what changed since our cursor (ListChanges).
//  2. Scan: list the local files (a full cycle only).
//  3. For every path touched on either side, compare three states (the last
//     agreed state, the local file, the server's version) and act: see
//     Decide.
//  4. Save the cursor, so the next pull starts where this one ended.
//
// Every step is safe to repeat. Decisions compare content, not remembered
// intentions, so after a crash at any point the next cycle sees what really
// happened and finishes the job instead of doing it twice.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/chunk"
	"github.com/dharmikchandel/go-sync/internal/client"
	"github.com/dharmikchandel/go-sync/internal/syncpath"
)

// StateDir holds the daemon's database inside the synced folder. It is never
// synced itself.
const StateDir = ".gosync"

type Options struct {
	// Device names this device in conflicted copies. Default: the hostname.
	Device string
	Logger *slog.Logger
	// PollInterval is how often Run asks the server for changes.
	PollInterval time.Duration
	// RescanInterval is how often Run scans the whole folder even without
	// file events, in case an event was missed.
	RescanInterval time.Duration
}

type Syncer struct {
	root  string
	c     *client.Client
	state *state
	opts  Options
	log   *slog.Logger
	now   func() time.Time
}

// errLocalChanged means the local file changed while we were working on it.
// The change is picked up by the next cycle.
var errLocalChanged = errors.New("local file changed during sync")

// Open prepares folder root for syncing as user. Only one Syncer can have a
// folder open at a time (ErrFolderInUse).
func Open(root string, c *client.Client, user string, opts Options) (*Syncer, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if opts.Device == "" {
		opts.Device, _ = os.Hostname()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 2 * time.Second
	}
	if opts.RescanInterval <= 0 {
		opts.RescanInterval = time.Minute
	}
	if err := os.MkdirAll(filepath.Join(root, StateDir), 0o755); err != nil {
		return nil, err
	}
	st, err := openState(filepath.Join(root, StateDir, "state.db"))
	if err != nil {
		return nil, err
	}
	if err := st.checkUser(user); err != nil {
		st.Close()
		return nil, err
	}
	s := &Syncer{root: root, c: c, state: st, opts: opts, log: opts.Logger, now: time.Now}
	// Temp files from a download that was interrupted by a crash. We hold the
	// folder lock, so none of them belong to a running download.
	s.removeTempFiles()
	return s, nil
}

func (s *Syncer) Close() error {
	return s.state.Close()
}

// SyncOnce runs one full cycle: every local file and every server change.
func (s *Syncer) SyncOnce(ctx context.Context) error {
	return s.cycle(ctx, true)
}

// cycle pulls server changes and reconciles them. If full, it also scans
// the folder and reconciles every local path.
func (s *Syncer) cycle(ctx context.Context, full bool) error {
	cursor, err := s.state.cursor()
	if err != nil {
		return err
	}
	changes, next, err := s.c.Changes(ctx, cursor)
	if err != nil {
		return fmt.Errorf("pull changes: %w", err)
	}

	// Server changes first, in feed order, then local-only paths.
	remote := make(map[string]*gosyncv1.FileVersion, len(changes))
	var paths []string
	for _, ch := range changes {
		remote[ch.Path] = ch
		paths = append(paths, ch.Path)
	}
	if full {
		local, err := s.scan()
		if err != nil {
			return err
		}
		// Known paths that are gone locally must be checked too: they may
		// have been deleted here.
		known, err := s.state.livePaths()
		if err != nil {
			return err
		}
		extra := append(local, known...)
		slices.Sort(extra)
		for _, p := range slices.Compact(extra) {
			if remote[p] == nil {
				paths = append(paths, p)
			}
		}
	}

	var (
		failed      int
		firstFailed int64 // lowest feed seq whose change wasn't applied
	)
	for _, p := range paths {
		err := s.syncPath(ctx, p, remote[p])
		if err == nil {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errLocalChanged) {
			s.log.Info("file changed while syncing, will retry", "path", p)
		} else {
			failed++
			s.log.Error("sync failed", "path", p, "err", err)
		}
		if ch := remote[p]; ch != nil && (firstFailed == 0 || ch.Seq < firstFailed) {
			firstFailed = ch.Seq
		}
	}

	// Move the cursor past every change we applied, but not past one we
	// didn't: the next pull must return it again.
	if firstFailed > 0 {
		next = firstFailed - 1
	}
	if next != cursor {
		if err := s.state.setCursor(next); err != nil {
			return err
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d file(s) failed to sync", failed)
	}
	return nil
}

// syncPath reconciles one path. change is its entry from the change feed,
// or nil if the server hasn't changed it since our cursor.
func (s *Syncer) syncPath(ctx context.Context, p string, change *gosyncv1.FileVersion) error {
	for range 3 {
		err := s.reconcile(ctx, p, change)
		var conflict *client.ConflictError
		if !errors.As(err, &conflict) {
			return err
		}
		// The server moved on after our pull, so our write was rejected.
		// Look at its current version and decide again.
		change, err = s.c.GetFile(ctx, p, 0)
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("%s: still conflicting after 3 attempts", p)
}

// remoteFile is the server's side of one path.
type remoteFile struct {
	Side
	version int64
	file    *gosyncv1.FileVersion // with its manifest; nil if unchanged or deleted
}

func (s *Syncer) reconcile(ctx context.Context, p string, change *gosyncv1.FileVersion) error {
	base, err := s.state.get(p)
	if err != nil {
		return err
	}
	local, info, err := s.localSide(p, base)
	if errors.Is(err, errNotRegular) {
		s.log.Warn("skipping: not a regular file", "path", p)
		return nil
	}
	if err != nil {
		return err
	}
	remote, err := s.remoteSide(ctx, p, base, change)
	if err != nil {
		return err
	}

	action := Decide(base.side(), local, remote.Side)
	switch action {
	case InSync:
		if remote.version == 0 {
			return nil // gone here and never on the server: nothing to record
		}
		next := Base{Path: p, Version: remote.version, Deleted: !remote.Exists}
		if remote.Exists {
			next.Content = remote.Content
			next.Size, next.ModTime = info.Size(), info.ModTime().UnixNano()
		}
		if next == base {
			return nil
		}
		return s.state.put(next)
	case Upload:
		return s.upload(ctx, p, remote.version)
	case DeleteRemote:
		return s.deleteRemote(ctx, p, remote.version)
	case Download:
		return s.download(ctx, p, remote.file, info)
	case DeleteLocal:
		return s.deleteLocal(p, remote.version, info)
	case Conflict:
		return s.conflict(ctx, p, remote.file, info)
	}
	panic("unknown action")
}

var errNotRegular = errors.New("not a regular file")

// localSide describes the local file at p. info is nil if it doesn't exist.
func (s *Syncer) localSide(p string, base Base) (Side, fs.FileInfo, error) {
	info, err := os.Lstat(s.abs(p))
	if errors.Is(err, fs.ErrNotExist) {
		return Side{}, nil, nil
	}
	if err != nil {
		return Side{}, nil, err
	}
	if !info.Mode().IsRegular() {
		return Side{}, nil, errNotRegular
	}
	// Same size and modification time as when we last synced: assume the
	// content is unchanged and skip reading it (git does the same).
	if base.side().Exists && info.Size() == base.Size && info.ModTime().UnixNano() == base.ModTime {
		return Side{Exists: true, Content: base.Content}, info, nil
	}
	f, err := os.Open(s.abs(p))
	if err != nil {
		return Side{}, nil, err
	}
	defer f.Close()
	hashes, err := chunk.Manifest(f)
	if err != nil {
		return Side{}, nil, err
	}
	return Side{Exists: true, Content: chunk.ContentID(hashes)}, info, nil
}

func (s *Syncer) remoteSide(ctx context.Context, p string, base Base, change *gosyncv1.FileVersion) (remoteFile, error) {
	if change == nil || change.Version == base.Version {
		return remoteFile{Side: base.side(), version: base.Version}, nil
	}
	if change.Deleted {
		return remoteFile{version: change.Version}, nil
	}
	// The feed has no manifests; fetch this version's. Asking for the exact
	// version (not "current") keeps the decision consistent even if the
	// file changes again meanwhile. That change shows up in the next pull.
	f, err := s.c.GetFile(ctx, p, change.Version)
	if err != nil {
		return remoteFile{}, err
	}
	return remoteFile{
		Side:    Side{Exists: true, Content: chunk.ContentID(client.Hashes(f))},
		version: f.Version,
		file:    f,
	}, nil
}

// upload sends the local file as the version after base.
func (s *Syncer) upload(ctx context.Context, p string, base int64) error {
	f, err := os.Open(s.abs(p))
	if err != nil {
		return err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return err
	}
	hashes, err := s.c.PutBlocks(ctx, f)
	if err != nil {
		return err
	}
	// If the file was written while we read it, the blocks may mix old and
	// new content. Don't commit that; the write will trigger another cycle.
	if !s.unchanged(p, before) {
		return errLocalChanged
	}
	v, err := s.c.Commit(ctx, p, base, hashes)
	if err != nil {
		return err
	}
	s.log.Info("uploaded", "path", p, "version", v.Version, "size", v.Size)
	return s.state.put(Base{
		Path: p, Version: v.Version, Content: chunk.ContentID(hashes),
		Size: before.Size(), ModTime: before.ModTime().UnixNano(),
	})
}

func (s *Syncer) deleteRemote(ctx context.Context, p string, base int64) error {
	v, err := s.c.Delete(ctx, p, base)
	if err != nil {
		return err
	}
	s.log.Info("deleted on server", "path", p, "version", v.Version)
	return s.state.put(Base{Path: p, Version: v.Version, Deleted: true})
}

// download replaces the local file (described by info, nil if absent) with
// the server's version f.
func (s *Syncer) download(ctx context.Context, p string, f *gosyncv1.FileVersion, info fs.FileInfo) error {
	dst := s.abs(p)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := s.c.Fetch(ctx, f, filepath.Dir(dst))
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // fails harmlessly once installed

	if info != nil {
		// Keep the local file's permissions.
		if err := os.Chmod(tmp, info.Mode().Perm()); err != nil {
			return err
		}
	}
	// The download took time. If the user edited the file meanwhile,
	// replacing it now would destroy that edit. (A tiny window remains
	// between this check and the rename; see the M2 notes.)
	if !s.unchanged(p, info) {
		return errLocalChanged
	}
	// Record the temp file's stat, not the installed one's: rename keeps
	// size and mtime, and an edit right after the rename must still look
	// like a change.
	tinfo, err := os.Stat(tmp)
	if err != nil {
		return err
	}
	if err := client.Install(tmp, dst); err != nil {
		return err
	}
	s.log.Info("downloaded", "path", p, "version", f.Version, "size", f.Size)
	return s.state.put(Base{
		Path: p, Version: f.Version, Content: chunk.ContentID(client.Hashes(f)),
		Size: tinfo.Size(), ModTime: tinfo.ModTime().UnixNano(),
	})
}

func (s *Syncer) deleteLocal(p string, version int64, info fs.FileInfo) error {
	if !s.unchanged(p, info) {
		return errLocalChanged
	}
	if err := os.Remove(s.abs(p)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	s.removeEmptyParents(p)
	s.log.Info("deleted locally", "path", p, "version", version)
	return s.state.put(Base{Path: p, Version: version, Deleted: true})
}

// conflict keeps both edits. The local file is renamed to a conflicted copy
// (a rename: nothing is copied, nothing can be half-written), then the
// server's version is downloaded under the original name. The copy is a new
// file, so it's uploaded and reaches every other device too.
//
// If we crash between the two steps, the next cycle sees the original name
// missing locally and the server's edit, and downloads it (edit beats delete).
func (s *Syncer) conflict(ctx context.Context, p string, f *gosyncv1.FileVersion, info fs.FileInfo) error {
	if !s.unchanged(p, info) {
		return errLocalChanged
	}
	copyPath, err := s.freeConflictName(p)
	if err != nil {
		return err
	}
	if err := os.Rename(s.abs(p), s.abs(copyPath)); err != nil {
		return err
	}
	s.log.Warn("conflict: both devices edited the file; kept this device's edit as a copy",
		"path", p, "copy", copyPath)
	if err := s.download(ctx, p, f, nil); err != nil {
		return err
	}
	return s.syncPath(ctx, copyPath, nil)
}

// freeConflictName returns an unused conflicted-copy name for p, like
// "notes (conflicted copy from laptop 2026-09-28 150405).txt".
func (s *Syncer) freeConflictName(p string) (string, error) {
	dir, name := path.Split(p)
	ext := path.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	if stem == "" { // a dotfile like ".bashrc" has no extension
		stem, ext = name, ""
	}
	device := strings.NewReplacer("/", "-", `\`, "-").Replace(s.opts.Device)
	label := fmt.Sprintf("conflicted copy from %s %s", device, s.now().Format("2006-01-02 150405"))
	for i := 1; ; i++ {
		suffix := label
		if i > 1 {
			suffix = fmt.Sprintf("%s %d", label, i)
		}
		candidate := fmt.Sprintf("%s%s (%s)%s", dir, stem, suffix, ext)
		if err := syncpath.Validate(candidate); err != nil {
			return "", err
		}
		if _, err := os.Lstat(s.abs(candidate)); errors.Is(err, fs.ErrNotExist) {
			return candidate, nil
		}
	}
}

// unchanged reports whether the local file at p still matches info (nil
// meaning "didn't exist"). Editors often save by writing a new file and
// renaming it over the old one, so identity is compared too, not just size
// and time.
func (s *Syncer) unchanged(p string, info fs.FileInfo) bool {
	now, err := os.Lstat(s.abs(p))
	if info == nil {
		return errors.Is(err, fs.ErrNotExist)
	}
	return err == nil && os.SameFile(info, now) &&
		now.Size() == info.Size() && now.ModTime().Equal(info.ModTime())
}

// scan returns the path of every regular file in the folder.
func (s *Syncer) scan() ([]string, error) {
	var paths []string
	err := filepath.WalkDir(s.root, func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if full == s.root {
			return nil
		}
		p, ignored := s.rel(full)
		switch {
		case ignored && d.IsDir():
			return filepath.SkipDir
		case ignored || !d.Type().IsRegular():
			return nil
		}
		if err := syncpath.Validate(p); err != nil {
			s.log.Warn("skipping file with unsupported name", "path", p, "err", err)
			return nil
		}
		paths = append(paths, p)
		return nil
	})
	return paths, err
}

// rel turns an absolute path into a sync path, and reports whether it's one
// the syncer ignores: the root itself, the state directory and download
// temp files.
func (s *Syncer) rel(full string) (p string, ignored bool) {
	r, err := filepath.Rel(s.root, full)
	if err != nil || r == "." {
		return "", true
	}
	p = filepath.ToSlash(r)
	return p, p == StateDir || strings.HasPrefix(p, StateDir+"/") ||
		strings.HasPrefix(path.Base(p), client.TempPrefix)
}

func (s *Syncer) abs(p string) string {
	return filepath.Join(s.root, filepath.FromSlash(p))
}

// removeEmptyParents removes the directories above p that are now empty, so
// a folder deleted on another device disappears here too.
func (s *Syncer) removeEmptyParents(p string) {
	for dir := path.Dir(p); dir != "."; dir = path.Dir(dir) {
		if os.Remove(s.abs(dir)) != nil { // fails if not empty
			return
		}
	}
}

func (s *Syncer) removeTempFiles() {
	filepath.WalkDir(s.root, func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == StateDir && filepath.Dir(full) == s.root {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), client.TempPrefix) {
			os.Remove(full)
		}
		return nil
	})
}
