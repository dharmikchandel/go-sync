package syncer

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"modernc.org/sqlite" // pure Go SQLite: no cgo, so builds stay static
	sqlite3 "modernc.org/sqlite/lib"
)

// Base is what this device and the server last agreed on for one path: the
// "base" of the three-way comparison in Decide.
type Base struct {
	Path    string
	Version int64 // server version (0: the server has never had this path)
	Deleted bool
	Content [32]byte
	// Size and ModTime (unix nanoseconds) of the local file when it was last
	// synced. If both still match, the file is unchanged and isn't re-read.
	Size    int64
	ModTime int64
}

func (b Base) side() Side {
	return Side{Exists: b.Version > 0 && !b.Deleted, Content: b.Content}
}

// state is the daemon's local database, kept in <folder>/.gosync/state.db.
type state struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
	path     TEXT PRIMARY KEY,
	version  INTEGER NOT NULL,
	deleted  INTEGER NOT NULL,
	content  BLOB NOT NULL,
	size     INTEGER NOT NULL,
	mtime_ns INTEGER NOT NULL
);`

var ErrFolderInUse = errors.New("another go-sync process is using this folder")

func openState(path string) (*state, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite allows one writer anyway, and the pragmas
	// below are per connection.
	db.SetMaxOpenConns(1)
	s := &state{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *state) init() error {
	for _, p := range []string{
		// Fail at once instead of waiting if another process holds the lock.
		"PRAGMA busy_timeout = 0",
		// Keep the file lock for as long as we're open. This is what stops
		// two daemons from syncing the same folder and fighting.
		"PRAGMA locking_mode = EXCLUSIVE",
		"PRAGMA journal_mode = WAL",
		// NORMAL can lose the last few commits on power loss (never corrupt).
		// That's safe here: every sync step is re-checked from scratch.
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := s.db.Exec(p); err != nil {
			return lockErr(err)
		}
	}
	// A write takes the exclusive lock now rather than at the first sync.
	if _, err := s.db.Exec(schema); err != nil {
		return lockErr(err)
	}
	_, err := s.db.Exec(`INSERT INTO meta VALUES ('opened', '1') ON CONFLICT DO UPDATE SET value = value + 1`)
	return lockErr(err)
}

func lockErr(err error) error {
	var se *sqlite.Error
	if errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_BUSY {
		return ErrFolderInUse
	}
	return err
}

func (s *state) Close() error {
	return s.db.Close()
}

// checkUser ties the folder to the first user that syncs it. Syncing it as
// someone else would compare this folder against the wrong user's files.
func (s *state) checkUser(user string) error {
	stored, err := s.getMeta("user")
	if err != nil {
		return err
	}
	if stored == "" {
		return s.setMeta("user", user)
	}
	if stored != user {
		return fmt.Errorf("this folder is synced as user %q, not %q", stored, user)
	}
	return nil
}

// cursor is the change-feed position this device has applied up to.
func (s *state) cursor() (int64, error) {
	v, err := s.getMeta("cursor")
	if err != nil || v == "" {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (s *state) setCursor(seq int64) error {
	return s.setMeta("cursor", strconv.FormatInt(seq, 10))
}

func (s *state) getMeta(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *state) setMeta(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO meta VALUES (?, ?) ON CONFLICT DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// get returns the base for path, or a zero Base (version 0) if unknown.
func (s *state) get(path string) (Base, error) {
	b := Base{Path: path}
	var content []byte
	err := s.db.QueryRow(
		`SELECT version, deleted, content, size, mtime_ns FROM files WHERE path = ?`, path,
	).Scan(&b.Version, &b.Deleted, &content, &b.Size, &b.ModTime)
	if errors.Is(err, sql.ErrNoRows) {
		return b, nil
	}
	copy(b.Content[:], content)
	return b, err
}

func (s *state) put(b Base) error {
	_, err := s.db.Exec(
		`INSERT INTO files (path, version, deleted, content, size, mtime_ns) VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (path) DO UPDATE SET version = excluded.version, deleted = excluded.deleted,
		   content = excluded.content, size = excluded.size, mtime_ns = excluded.mtime_ns`,
		b.Path, b.Version, b.Deleted, b.Content[:], b.Size, b.ModTime)
	return err
}

// livePaths returns every path the device believes exists.
func (s *state) livePaths() ([]string, error) {
	rows, err := s.db.Query(`SELECT path FROM files WHERE NOT deleted`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}
