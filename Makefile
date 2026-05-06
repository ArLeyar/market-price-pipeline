.PHONY: build run test integration lint up down logs

build:
	go build -o bin/server ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./internal/... ./cmd/...

integration:
	go test -tags=integration -timeout=300s ./tests/...

e2e:
	go test -tags=e2e -timeout=120s -v ./tests/...

e2e-clean:
	docker compose -f deploy/docker-compose.yml --env-file .env down -v

up:
	docker compose -f deploy/docker-compose.yml --env-file .env up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

logs:
	docker compose -f deploy/docker-compose.yml logs -f server

tidy:
	go mod tidy
