package metadata

import (
	"context"
	"database/sql"
	// "fmt"
	"time"

	_ "github.com/lib/pq"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(conn string) (*Repository, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return &Repository{db: db}, nil
}

func (r *Repository) GetOrCreateFile(ctx context.Context, owner, filename string) (string, int, error) {
	var id string
	var version int

	err := r.db.QueryRowContext(ctx,
		`SELECT id, current_version FROM files WHERE owner_id=$1 AND filename=$2`,
		owner, filename,
	).Scan(&id, &version)

	if err == sql.ErrNoRows {
		err = r.db.QueryRowContext(ctx,
			`INSERT INTO files(owner_id, filename) VALUES ($1,$2) RETURNING id, current_version`,
			owner, filename,
		).Scan(&id, &version)
	}

	return id, version, err
}

func (r *Repository) InsertVersion(
	ctx context.Context,
	fileID string,
	version int,
	key string,
	checksum string,
	size int64,
) error {

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO file_versions
		 (file_id, version_number, storage_key, checksum, size_bytes)
		 VALUES ($1,$2,$3,$4,$5)`,
		fileID, version, key, checksum, size,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE files SET current_version=$1 WHERE id=$2`,
		version, fileID,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (r *Repository) NextVersion(current int) int {
	return current + 1
}

func (r *Repository) Close() {
	r.db.Close()
}

func (r *Repository) InsertVersionSafe(
	ctx context.Context,
	fileID string,
	expectedCurrent int,
	newVersion int,
	key string,
	checksum string,
	size int64,
) (bool, error) {

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE files 
		 SET current_version=$1 
		 WHERE id=$2 AND current_version=$3`,
		newVersion, fileID, expectedCurrent,
	)
	if err != nil {
		tx.Rollback()
		return false, err
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		tx.Rollback()
		return false, nil // version changed by another writer
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO file_versions
		 (file_id, version_number, storage_key, checksum, size_bytes)
		 VALUES ($1,$2,$3,$4,$5)`,
		fileID, newVersion, key, checksum, size,
	)
	if err != nil {
		tx.Rollback()
		return false, err
	}

	return true, tx.Commit()
}

type FileVersion struct {
	ID         string
	StorageKey string
	CreatedAt  time.Time
}

func (r *Repository) ListAllVersions(ctx context.Context) ([]FileVersion, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, storage_key, created_at FROM file_versions`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FileVersion

	for rows.Next() {
		var v FileVersion
		if err := rows.Scan(&v.ID, &v.StorageKey, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}

	return out, nil
}

func (r *Repository) ListVersionsForFile(ctx context.Context, fileID string) ([]FileVersion, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, storage_key, created_at
		 FROM file_versions
		 WHERE file_id = $1
		 ORDER BY created_at ASC`,
		fileID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []FileVersion

	for rows.Next() {
		var v FileVersion
		if err := rows.Scan(&v.ID, &v.StorageKey, &v.CreatedAt); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}

	return versions, nil
}

func (r *Repository) DeleteVersion(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM file_versions WHERE id=$1`, id,
	)
	return err
}

type File struct {
	ID string
}

func (r *Repository) ListFiles(ctx context.Context) ([]File, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id FROM files`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File

	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID); err != nil {
			return nil, err
		}
		files = append(files, f)
	}

	return files, nil
}

func (r *Repository) GetLatestVersion(
	ctx context.Context,
	filename string,
) (string, string, error) {

	var fileID string
	var currentVersion int

	err := r.db.QueryRowContext(ctx,
		`SELECT id, current_version
		 FROM files
		 WHERE owner_id=$1 AND filename=$2`,
		"user1", filename,
	).Scan(&fileID, &currentVersion)
	if err != nil {
		return "", "", err
	}

	var key string

	err = r.db.QueryRowContext(ctx,
		`SELECT storage_key
		 FROM file_versions
		 WHERE file_id=$1 AND version_number=$2`,
		fileID, currentVersion,
	).Scan(&key)
	if err != nil {
		return "", "", err
	}

	return fileID, key, nil
}