# go-sync

A file sync engine in Go: a server that stores versioned files, and a client that keeps folders in sync across devices.

> **Status:** early rebuild. Currently the project scaffold is in place (server, CLI, Postgres, S3 storage, CI). Sync features are in progress.

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
internal/config   env-based configuration
internal/db       Postgres pool + embedded migrations
internal/blob     S3 object storage
internal/server   gRPC service implementation
internal/testenv  throwaway containers for integration tests
```
