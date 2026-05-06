.PHONY: build run test integration e2e e2e-clean lint vet cover up down logs tidy help

help:
	@echo "Targets:"
	@echo "  build        compile server binary into bin/"
	@echo "  run          go run ./cmd/server"
	@echo "  test         unit tests with race detector"
	@echo "  cover        unit tests + coverage summary"
	@echo "  integration  testcontainers (postgres+redis+kafka)"
	@echo "  e2e          live compose stack tests (requires up)"
	@echo "  e2e-clean    docker compose down -v"
	@echo "  lint         golangci-lint run"
	@echo "  vet          go vet across all build tags"
	@echo "  up           docker compose up --build"
	@echo "  down         docker compose down -v"
	@echo "  logs         tail server logs"
	@echo "  tidy         go mod tidy"

build:
	go build -o bin/server ./cmd/server

run:
	go run ./cmd/server

test:
	go test -race -count=1 ./internal/... ./cmd/...

cover:
	go test -race -count=1 -coverprofile=coverage.out -covermode=atomic ./internal/... ./cmd/...
	go tool cover -func=coverage.out | tail -20

integration:
	go test -tags=integration -timeout=300s ./tests/...

e2e:
	go test -tags=e2e -timeout=120s -v ./tests/...

e2e-clean:
	docker compose -f deploy/docker-compose.yml --env-file .env down -v

lint:
	golangci-lint run --timeout=5m

vet:
	go vet ./...
	go vet -tags=integration ./tests/...
	go vet -tags=e2e ./tests/...

up:
	docker compose -f deploy/docker-compose.yml --env-file .env up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

logs:
	docker compose -f deploy/docker-compose.yml logs -f server

tidy:
	go mod tidy
