# CLAUDE.md

Guidance for AI agents (Claude Code, Codex, Cursor, etc.) working in this repo.
For human-readable docs see [`README.md`](README.md) and [`task.md`](task.md).

## Run / test / lint

| Goal | Command |
|---|---|
| Build server | `make build` |
| Run server locally | `make run` |
| Bring up full stack | `make up` (then `make down` / `make logs`) |
| Unit tests + race | `make test` |
| Unit + coverage summary | `make cover` |
| Vet across all build tags | `make vet` |
| Lint | `make lint` (golangci-lint v2.12+, config: `.golangci.yml`) |
| Integration tests (testcontainers) | `make integration` |
| E2E tests against live compose | `make e2e` (requires `make up`) |

## Layout — one line per package

| Package | Responsibility |
|---|---|
| `cmd/server` | DI + signal-bound shutdown; HTTP server + WS goroutine |
| `internal/binance` | WebSocket subscription, parser, reconnect with jittered backoff |
| `internal/config` | env parsing via `envconfig`, validation (empty filter, required fields) |
| `internal/domain` | `Price` value type only — no logic |
| `internal/http` | chi router, handlers, request/response DTOs |
| `internal/kafka` | producer wrapper around `segmentio/kafka-go`, cached `Ping` |
| `internal/pipeline` | `HandleTick` — sequential write to DB / cache / events with per-sink timeouts |
| `internal/storage` | Postgres (`Save`, `GetLatest`, `GetHistory`, `Ping`) and Redis (`SetLatest`, `GetLatest`, `Ping`) |
| `migrations/` | `golang-migrate`-compatible SQL files |
| `tests/` | `integration_test.go` (testcontainers, build tag `integration`); `e2e_test.go` (live compose, build tag `e2e`) |
| `deploy/` | `Dockerfile` (multi-stage, distroless), `docker-compose.yml` |

## Conventions

- **Errors:** wrap with `fmt.Errorf("context: %w", err)`. Don't return bare errors from external calls. Use `errors.Is` / `errors.As` for branching.
- **Comments:** comment *why*, never *what*. Self-explanatory code over restating it. Section dividers (`// --- foo ---`) are fine in long test files.
- **Context:** every external call (DB, Redis, Kafka, HTTP) takes `ctx`. Public exported methods accept ctx as the first arg. Per-sink timeouts live in `pipeline.go` constants.
- **Time bounds:** any call into an external dependency must have a timeout, either via `context.WithTimeout` or a client-level deadline. Never trust a parent ctx without a deadline.
- **Interfaces:** define small interfaces in the **consumer** package (e.g. `httpapi.Store`, `pipeline.DBSink`). Avoid global "all-methods-on-everything" interfaces. The persistence package exposes concrete types; consumers depend on subset interfaces.
- **No mocks by hand for non-trivial types:** prefer testcontainers for DB/Kafka/Redis. Hand-rolled inline fakes (single-method stubs) are fine for unit tests of HTTP handlers and pipeline semantics.
- **Test build tags:** `integration` for testcontainers, `e2e` for live-stack. Default `go test ./...` is unit-only.

## Architectural invariants (do not break without an ADR update in README)

1. **Postgres is source of truth.** Any new sink is best-effort and must not block ingest on failure.
2. **`HandleTick` is sequential and short.** No goroutine fan-out, no unbounded buffering. Per-sink timeouts only.
3. **Stable order `(ts, id)` for `/latest` and `/history`.** Ticks may share a timestamp.
4. **`/health` returns 503 if any dependency is down.** It is a readiness check, not liveness.
5. **WS reconnect backoff resets on first message in a session.** Otherwise long sessions + drops pin reconnect at maxBackoff.
6. **`/prices/history` is bounded** — 7-day window cap and 10k row limit. Removing these requires re-justifying DoS posture.

## Extending the system

See README sections **"Architecture decisions"** and **"Extending the system"** for canonical guidance.

- New exchange → implement same shape as `internal/binance/stream.go`; wire in `cmd/server/main.go` as another goroutine; pipeline / schema / API need no changes.
- New endpoint → add method on `Handlers`, register route in `router.go`. If new repo method is needed, extend the `Store` interface (one Go file, one fake to update).
- New persistence backend → implement `httpapi.Store` and `pipeline.DBSink`. Both are 3-method interfaces.
- Adding metrics → register Prometheus collectors in `cmd/server/main.go`, mount `/metrics` on the chi router. Pass collectors as fields where used.

## CI gates

`.github/workflows/ci.yml` runs on every PR:

- `lint` — golangci-lint v2 (`.golangci.yml`, 14 linters, including `errcheck`, `staticcheck`, `bodyclose`, `errorlint`, `sqlclosecheck`)
- `unit` — `go vet` (default + integration + e2e tags), `go build`, `go test -race -cover`
- `integration` — `go test -tags=integration` with testcontainers

A PR comment with coverage summary is posted by CI on each push.

**Before opening a PR**: run `make lint && make vet && make test && make integration` locally.

## What not to do

- Do not push to `main` directly; PRs only.
- Do not skip the lint step locally — CI will block.
- Do not add comments that restate the code (`// First reading`, `// Set the foo`).
- Do not introduce new dependencies without a strong reason — Go stdlib first.
- Do not weaken the architectural invariants above without updating the ADR table in README.
- Do not log raw external payloads (WS frames, Kafka messages) at INFO/WARN without truncation — see `truncate` in `internal/binance/stream.go`.
