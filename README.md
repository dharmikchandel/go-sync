# go-sync

A file sync engine in Go: a server that stores versioned files, and a client that keeps folders in sync across devices.

> **Status:** in progress. The versioned file store and CLI work; the folder-syncing daemon is next.

## Stack

- **Go** server and CLI, talking over **gRPC**
- **PostgreSQL** for metadata (source of truth)
- **S3-compatible object storage** for file content ([RustFS](https://github.com/rustfs/rustfs) locally)
- **Docker Compose** for the local environment

## Quick start

Requires Docker and Go 1.26+.

```bash
make up                      # build and start everything, wait until healthy
go run ./cmd/go-sync info    # -> server <version> (replica server-1)
make down                    # stop (make clean also deletes data)
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
```

`-base` is the version your copy is based on. If the server has moved past it, the write is rejected instead of overwriting someone else's change.

## How it works

- Files are split into **4 MiB blocks** named by their **SHA-256**. A version is a **manifest**: the ordered list of its block hashes.
- Uploading = store each block (in parallel), then **commit** the manifest with the base version. The commit is a **compare-and-swap** in Postgres, so concurrent edits can't silently overwrite each other.
- Every commit gets a **per-user, gap-free sequence number**. Devices ask "what changed since N?" to catch up after being offline. Deletes are recorded as **tombstone versions**, so they propagate too.
- Block bytes go to object storage **before** metadata is committed, and a foreign key makes Postgres reject manifests that reference unstored blocks. A crash can leave unused bytes, never a broken file.
- Blocks are verified by hash on upload (server) and download (client). Downloads are written to a temp file, fsynced and atomically renamed, so a failed download never damages the local copy.

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
internal/syncpath path validation shared by client and server
internal/testenv  throwaway containers for integration tests
```
