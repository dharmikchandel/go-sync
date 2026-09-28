-- +goose Up

-- change_seq is the user's change-feed counter. Each commit increments it
-- while holding the user's row lock, so a user's commits get gap-free,
-- strictly increasing seqs in the same order they become visible.
ALTER TABLE users ADD COLUMN change_seq BIGINT NOT NULL DEFAULT 0;

-- A block is a piece of file content, identified by the SHA-256 of its bytes.
-- A row here means the bytes are already in object storage (written first).
-- Blocks are scoped per user: global dedup would let one user test whether
-- another user has a given file.
CREATE TABLE blocks (
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    hash       BYTEA       NOT NULL CHECK (length(hash) = 32),
    size       INT         NOT NULL CHECK (size >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, hash)
);

-- files holds one row per path, pointing at its current version.
-- current_version is the compare-and-swap target for commits.
-- current_seq duplicates the current version's seq so the change feed is a
-- single index range scan.
CREATE TABLE files (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users (id),
    path            TEXT   NOT NULL,
    current_version BIGINT NOT NULL,
    current_seq     BIGINT NOT NULL,
    UNIQUE (user_id, path)
);
CREATE INDEX files_user_seq ON files (user_id, current_seq);

-- file_versions is append-only history. A tombstone (deleted = true) is a
-- version too, so deletes propagate through the change feed like edits.
CREATE TABLE file_versions (
    file_id    BIGINT      NOT NULL REFERENCES files (id),
    version    BIGINT      NOT NULL,
    seq        BIGINT      NOT NULL,
    deleted    BOOLEAN     NOT NULL,
    size       BIGINT      NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (file_id, version)
);

-- version_blocks is each version's manifest. The foreign key to blocks means
-- the database itself refuses to reference a block that isn't stored, and
-- refuses to delete a block that some version still uses.
CREATE TABLE version_blocks (
    file_id BIGINT NOT NULL,
    version BIGINT NOT NULL,
    idx     INT    NOT NULL,
    user_id BIGINT NOT NULL,
    hash    BYTEA  NOT NULL,
    PRIMARY KEY (file_id, version, idx),
    FOREIGN KEY (file_id, version) REFERENCES file_versions (file_id, version),
    FOREIGN KEY (user_id, hash) REFERENCES blocks (user_id, hash)
);
CREATE INDEX version_blocks_block ON version_blocks (user_id, hash);

-- +goose Down
DROP TABLE version_blocks;
DROP TABLE file_versions;
DROP TABLE files;
DROP TABLE blocks;
ALTER TABLE users DROP COLUMN change_seq;
