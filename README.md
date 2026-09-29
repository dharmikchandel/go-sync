# go-sync

A file sync engine in Go: a server that stores versioned files, and a client that keeps folders in sync across devices.

> **Status:** in progress. The versioned file store, CLI and folder-syncing daemon work; block-level dedup on upload is next.

## Stack

- **Go** server and CLI, talking over **gRPC**
- **PostgreSQL** for metadata (source of truth)
- **S3-compatible object storage** for file content ([RustFS](https://github.com/rustfs/rustfs) locally)
- **SQLite** (pure Go) for each synced folder's local state, **fsnotify** for file events
- **Docker Compose** for the local environment

## Quick start

Requires Docker and Go 1.26+.

```bash
make up                      # build and start everything, wait until healthy
go run ./cmd/go-sync info    # -> server <version> (replica server-1)
make down                    # stop (make clean also deletes data)
```

Sync two folders, as if they were two devices:

```bash
go build -o bin/ ./cmd/...
bin/go-sync daemon -device laptop ~/sync-laptop    # terminal 1
bin/go-sync daemon -device phone  ~/sync-phone     # terminal 2
echo hi > ~/sync-laptop/notes.txt                  # appears in ~/sync-phone
```

## CLI

```
go-sync put [-base N] <local> [remote]     upload as a new version
go-sync get [-version N] <remote> [local]  download the current or an old version
go-sync rm [-base N] <remote>              delete (history is kept)
go-sync ls                                 list files
go-sync history <remote>                   list a file's versions
go-sync restore <remote> <version>         make an old version current (no re-upload)
go-sync changes [-since N]                 the change feed after cursor N
go-sync sync [-device name] <dir>          sync a folder once
go-sync daemon [-device name] <dir>        keep a folder in sync (watches for changes)
```

`-base` is the version your copy is based on. If the server has moved past it, the write is rejected instead of overwriting someone else's change.

## How it works

- Files are split into **4 MiB blocks** named by their **SHA-256**. A version is a **manifest**: the ordered list of its block hashes.
- Uploading = store each block (in parallel), then **commit** the manifest with the base version. The commit is a **compare-and-swap** in Postgres, so concurrent edits can't silently overwrite each other.
- Every commit gets a **per-user, gap-free sequence number**. Devices ask "what changed since N?" to catch up after being offline. Deletes are recorded as **tombstone versions**, so they propagate too.
- Block bytes go to object storage **before** metadata is committed, and a foreign key makes Postgres reject manifests that reference unstored blocks. A crash can leave unused bytes, never a broken file.
- Blocks are verified by hash on upload (server) and download (client). Downloads are written to a temp file, fsynced and atomically renamed, so a failed download never damages the local copy.

### The sync daemon

- Each folder keeps a small SQLite database (`.gosync/state.db`) with the **last state it agreed on with the server** for every file, plus its change-feed cursor.
- A sync cycle pulls server changes, scans the folder, and runs a **three-way comparison** per file: last agreed state vs local file vs server version. That tells who changed what:
  - only one side changed: copy the change across (upload, download, or delete);
  - both changed the same way: nothing to do;
  - one side deleted and the other edited: **the edit wins** (no work is lost);
  - both edited differently: **conflict**. The local edit is kept as `name (conflicted copy from <device> <time>).ext` and synced like any other file.
- Decisions compare **content** (a hash of the block list), not remembered intentions, so every step is safe to repeat. A crash at any point is fixed by the next cycle, with no duplicate uploads and no false conflicts.
- File events (fsnotify) only trigger a sync; a folder scan decides what changed, so a missed event can delay a sync but never break one. Files that change while being uploaded or downloaded are left alone and retried.
- The cursor never moves past a server change that failed to apply, so failures are retried, not skipped.

## Development

```bash
make test        # all tests, including integration tests against real Postgres + S3 (needs Docker)
make test-unit   # tests that don't need Docker
make lint        # go vet + staticcheck
make gen         # regenerate protobuf code (committed under gen/)
```

## Layout

```
cmd/server        gRPC server
cmd/go-sync       CLI client
proto/            API definition (protobuf)
gen/              generated code (committed)
internal/api      constants shared by client and server
internal/blob     S3 object storage
internal/chunk    splitting files into blocks
internal/client   client library (used by the CLI)
internal/config   env-based configuration
internal/db       Postgres pool + embedded migrations
internal/meta     metadata: commits, versions, change feed
internal/server   gRPC service implementation
internal/syncer   the sync daemon: state, three-way reconcile, file watching
internal/syncpath path validation shared by client and server
internal/testenv  throwaway containers for integration tests
```
