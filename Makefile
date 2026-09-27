VERSION ?= $(shell git describe --tags --always --dirty --exclude 'v0-*' 2>/dev/null || echo dev)
export VERSION

.PHONY: up down logs psql gen lint test test-unit build clean

## up: build and start the full stack, waiting until every service is healthy
up:
	docker compose up --build --wait

## down: stop the stack (keeps data volumes)
down:
	docker compose down

## clean: stop the stack and delete all data
clean:
	docker compose down -v

logs:
	docker compose logs -f server

## psql: open a SQL shell in the stack's Postgres
psql:
	docker compose exec postgres psql -U gosync -d gosync

## gen: regenerate protobuf code (the output in gen/ is committed)
gen:
	go tool buf lint
	go tool buf generate

lint:
	go vet ./...
	go tool staticcheck ./...

## test: all tests, including integration tests (needs Docker)
test:
	go test -race ./...

## test-unit: tests that don't need Docker
test-unit:
	go test -race -short ./...

build:
	go build -ldflags "-X main.version=$(VERSION)" -o bin/ ./cmd/...
