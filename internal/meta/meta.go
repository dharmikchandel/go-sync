// Package meta stores file metadata in Postgres: users, blocks, files, their
// versions and the per-user change feed. Postgres is the source of truth: a
// file version exists if and only if it is committed here.
package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrAlreadyDeleted = errors.New("file is already deleted")
)

// ConflictError means the caller's base version is not the file's current
// version: someone else committed first, so the caller's edit is stale.
type ConflictError struct {
	Current int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("version conflict: current version is %d", e.Current)
}

// MissingBlocksError means a commit referenced blocks that aren't stored.
type MissingBlocksError struct {
	Hashes [][]byte
}

func (e *MissingBlocksError) Error() string {
	return fmt.Sprintf("%d block(s) not uploaded", len(e.Hashes))
}

type Block struct {
	Hash []byte
	Size int32
}

type FileVersion struct {
	Path      string
	Version   int64
	Seq       int64
	Deleted   bool
	Size      int64
	CreatedAt time.Time
	Blocks    []Block // only filled by GetFile
}

type Repo struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// EnsureUser returns the user's ID, creating the user on first sight.
func (r *Repo) EnsureUser(ctx context.Context, name string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `SELECT id FROM users WHERE name = $1`, name).Scan(&id)
	if err == nil || !errors.Is(err, pgx.ErrNoRows) {
		return id, err
	}
	// ON CONFLICT covers two first requests for the same user racing: the
	// loser's insert does nothing and its update returns the winner's row.
	err = r.pool.QueryRow(ctx,
		`INSERT INTO users (name) VALUES ($1)
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, name).Scan(&id)
	return id, err
}

func (r *Repo) HasBlock(ctx context.Context, userID int64, hash []byte) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM blocks WHERE user_id = $1 AND hash = $2)`,
		userID, hash).Scan(&exists)
	return exists, err
}

// AddBlock records a block whose bytes are already in object storage.
// Idempotent, because two uploads of the same block may race.
func (r *Repo) AddBlock(ctx context.Context, userID int64, hash []byte, size int) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO blocks (user_id, hash, size) VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING`, userID, hash, size)
	return err
}

// Commit creates version BaseVersion+1 of a file. Deleted commits a
// tombstone and must have no blocks.
type Commit struct {
	UserID      int64
	Path        string
	BaseVersion int64
	Deleted     bool
	Blocks      [][]byte
}

// Commit records a new file version, or returns *ConflictError if c.BaseVersion
// isn't the current version, or *MissingBlocksError if a block isn't stored.
func (r *Repo) Commit(ctx context.Context, c Commit) (FileVersion, error) {
	if !c.Deleted {
		// Checked up front for a clear error listing every missing block. The
		// foreign key below is what actually guarantees it, even if a block
		// disappears between this check and the insert.
		if missing, err := r.missingBlocks(ctx, c.UserID, c.Blocks); err != nil {
			return FileVersion{}, err
		} else if len(missing) > 0 {
			return FileVersion{}, &MissingBlocksError{Hashes: missing}
		}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return FileVersion{}, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	// 1. Take the next change-feed seq. This UPDATE row-locks the user until
	// the transaction ends, so this user's commits run one at a time and each
	// seq becomes visible in order. With a global counter instead, seq 11
	// could become visible before seq 10 commits, and a client reading the
	// feed in between would skip 10 forever.
	var seq int64
	err = tx.QueryRow(ctx,
		`UPDATE users SET change_seq = change_seq + 1 WHERE id = $1 RETURNING change_seq`,
		c.UserID).Scan(&seq)
	if err != nil {
		return FileVersion{}, fmt.Errorf("allocate seq: %w", err)
	}

	// 2. Compare-and-swap the file's current version from base to base+1.
	newVersion := c.BaseVersion + 1
	fileID, err := casVersion(ctx, tx, c, newVersion, seq)
	if err != nil {
		return FileVersion{}, err
	}

	// 3. Record the version. Its size is computed from the stored block
	// sizes rather than trusted from the client.
	var v FileVersion
	err = tx.QueryRow(ctx,
		`INSERT INTO file_versions (file_id, version, seq, deleted, size)
		 VALUES ($1, $2, $3, $4, (
		     SELECT coalesce(sum(b.size), 0)
		     FROM unnest($5::bytea[]) AS m(hash)
		     JOIN blocks b ON b.user_id = $6 AND b.hash = m.hash))
		 RETURNING version, seq, deleted, size, created_at`,
		fileID, newVersion, seq, c.Deleted, c.Blocks, c.UserID,
	).Scan(&v.Version, &v.Seq, &v.Deleted, &v.Size, &v.CreatedAt)
	if err != nil {
		return FileVersion{}, fmt.Errorf("insert version: %w", err)
	}
	v.Path = c.Path

	// 4. Record the manifest in file order.
	_, err = tx.Exec(ctx,
		`INSERT INTO version_blocks (file_id, version, idx, user_id, hash)
		 SELECT $1, $2, m.ord - 1, $3, m.hash
		 FROM unnest($4::bytea[]) WITH ORDINALITY AS m(hash, ord)`,
		fileID, newVersion, c.UserID, c.Blocks)
	if isForeignKeyViolation(err) {
		return FileVersion{}, &MissingBlocksError{}
	}
	if err != nil {
		return FileVersion{}, fmt.Errorf("insert manifest: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return FileVersion{}, err
	}
	return v, nil
}

// casVersion moves the file at c.Path from c.BaseVersion to newVersion, or
// explains why it can't. It returns the file's ID.
func casVersion(ctx context.Context, tx pgx.Tx, c Commit, newVersion, seq int64) (int64, error) {
	var fileID int64

	if c.BaseVersion == 0 {
		// Base 0 means "I believe this file doesn't exist yet".
		if c.Deleted {
			return 0, ErrNotFound
		}
		err := tx.QueryRow(ctx,
			`INSERT INTO files (user_id, path, current_version, current_seq)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (user_id, path) DO NOTHING
			 RETURNING id`,
			c.UserID, c.Path, newVersion, seq).Scan(&fileID)
		if err == nil {
			return fileID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
		// The file exists, so the caller's belief is stale.
		return 0, conflict(ctx, tx, c)
	}

	// The WHERE clause is the compare-and-swap: it only matches if nobody
	// has moved the file past the caller's base version.
	var wasDeleted bool
	err := tx.QueryRow(ctx,
		`UPDATE files f SET current_version = $4, current_seq = $5
		 FROM file_versions v
		 WHERE f.user_id = $1 AND f.path = $2 AND f.current_version = $3
		   AND v.file_id = f.id AND v.version = f.current_version
		 RETURNING f.id, v.deleted`,
		c.UserID, c.Path, c.BaseVersion, newVersion, seq).Scan(&fileID, &wasDeleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, conflict(ctx, tx, c)
	}
	if err != nil {
		return 0, err
	}
	if c.Deleted && wasDeleted {
		return 0, ErrAlreadyDeleted
	}
	return fileID, nil
}

// conflict builds the error for a failed compare-and-swap, reporting the
// file's actual current version (0 if the file doesn't exist).
func conflict(ctx context.Context, tx pgx.Tx, c Commit) error {
	var current int64
	err := tx.QueryRow(ctx,
		`SELECT current_version FROM files WHERE user_id = $1 AND path = $2`,
		c.UserID, c.Path).Scan(&current)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	return &ConflictError{Current: current}
}

func (r *Repo) missingBlocks(ctx context.Context, userID int64, hashes [][]byte) ([][]byte, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT m.hash
		 FROM unnest($2::bytea[]) AS m(hash)
		 WHERE NOT EXISTS (SELECT 1 FROM blocks b WHERE b.user_id = $1 AND b.hash = m.hash)`,
		userID, hashes)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[[]byte])
}

// GetFile returns a version of a file with its manifest. Version 0 means the
// current version.
func (r *Repo) GetFile(ctx context.Context, userID int64, path string, version int64) (FileVersion, error) {
	var (
		v      = FileVersion{Path: path}
		fileID int64
	)
	err := r.pool.QueryRow(ctx,
		`SELECT f.id, v.version, v.seq, v.deleted, v.size, v.created_at
		 FROM files f
		 JOIN file_versions v ON v.file_id = f.id
		 WHERE f.user_id = $1 AND f.path = $2
		   AND v.version = CASE WHEN $3 = 0 THEN f.current_version ELSE $3 END`,
		userID, path, version,
	).Scan(&fileID, &v.Version, &v.Seq, &v.Deleted, &v.Size, &v.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return FileVersion{}, ErrNotFound
	}
	if err != nil {
		return FileVersion{}, err
	}

	rows, err := r.pool.Query(ctx,
		`SELECT vb.hash, b.size
		 FROM version_blocks vb
		 JOIN blocks b ON b.user_id = vb.user_id AND b.hash = vb.hash
		 WHERE vb.file_id = $1 AND vb.version = $2
		 ORDER BY vb.idx`,
		fileID, v.Version)
	if err != nil {
		return FileVersion{}, err
	}
	v.Blocks, err = pgx.CollectRows(rows, pgx.RowToStructByPos[Block])
	return v, err
}

// ListVersions returns a file's history, newest first.
func (r *Repo) ListVersions(ctx context.Context, userID int64, path string) ([]FileVersion, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT f.path, v.version, v.seq, v.deleted, v.size, v.created_at
		 FROM files f
		 JOIN file_versions v ON v.file_id = f.id
		 WHERE f.user_id = $1 AND f.path = $2
		 ORDER BY v.version DESC`,
		userID, path)
	if err != nil {
		return nil, err
	}
	versions, err := collectVersions(rows)
	if err == nil && len(versions) == 0 {
		err = ErrNotFound
	}
	return versions, err
}

// ListChanges returns up to limit files whose current version has seq > since,
// ordered by seq. Only current versions are returned, so each file appears at
// most once, and a file edited during paging moves to a later page instead of
// being missed.
func (r *Repo) ListChanges(ctx context.Context, userID, since int64, limit int) ([]FileVersion, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT f.path, v.version, v.seq, v.deleted, v.size, v.created_at
		 FROM files f
		 JOIN file_versions v ON v.file_id = f.id AND v.version = f.current_version
		 WHERE f.user_id = $1 AND f.current_seq > $2
		 ORDER BY f.current_seq
		 LIMIT $3`,
		userID, since, limit)
	if err != nil {
		return nil, err
	}
	return collectVersions(rows)
}

func collectVersions(rows pgx.Rows) ([]FileVersion, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (FileVersion, error) {
		var v FileVersion
		err := row.Scan(&v.Path, &v.Version, &v.Seq, &v.Deleted, &v.Size, &v.CreatedAt)
		return v, err
	})
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
