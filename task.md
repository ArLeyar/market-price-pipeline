# Original task

The project was built to satisfy this assignment:

> Build a production-like **Market Price Pipeline** service.
>
> The service must:
> 1. Periodically fetch prices for BTCUSDT, ETHUSDT, etc. from Binance.
> 2. Persist the received prices to a database.
> 3. After persisting, publish an event to Kafka.
> 4. Expose an HTTP API:
>    - `GET /health`
>    - `GET /prices/latest?symbol=BTCUSDT`
>    - `GET /prices/history?symbol=BTCUSDT&from=2026-05-05T09:00:00Z&to=2026-05-05T10:00:00Z`
> 5. Start with a single command:
>    ```
>    docker compose up --build
>    ```
> 6. Include integration tests.
> 7. Include a README with run instructions, test instructions, curl examples, and an architecture description.
>
> **Expected outcome:**
> - the service actually starts;
> - data is written to the database;
> - events are published to Kafka;
> - the API returns `latest` and `history`;
> - tests can be run by following the README.

## How the implementation maps to the requirements

| # | Requirement | Implementation |
|---|---|---|
| 1 | Fetch prices from Binance | `internal/binance/stream.go` — combined-stream WebSocket subscription for `bookTicker` updates of all configured symbols, with reconnect/backoff |
| 2 | Persist to DB | `internal/storage/postgres.go` — `Save` per tick into `prices` table (`migrations/0001_init.up.sql`) |
| 3 | Publish event to Kafka after persisting | `internal/pipeline/pipeline.go` — sequential write order Postgres → Redis → Kafka; Postgres is source of truth, Redis/Kafka are best-effort |
| 4 | HTTP API | `internal/http/handlers.go` — `/health`, `/prices/latest`, `/prices/history` |
| 5 | Single-command startup | `make up` (which expands to `docker compose -f deploy/docker-compose.yml --env-file .env up --build`). The compose file lives under `deploy/` so the repo root stays minimal. |
| 6 | Integration tests | `tests/integration_test.go` (testcontainers, isolated) and `tests/e2e_test.go` (live compose stack + real Binance) |
| 7 | README | `README.md` — quick start, env table, curl examples, architecture decisions, phased roadmap, extension guide |

## Beyond the assignment

Items added on top of the minimum requirements:

- **Redis cache** in front of `/prices/latest` for sub-millisecond reads, with cache warm on DB fallback.
- **Resilient WS loop** with exponential backoff that resets after first message.
- **Per-sink timeouts** in the pipeline so a wedged dependency cannot block ingest.
- **Bounded graceful shutdown** so a stuck WS read cannot outlive the SIGTERM grace period.
- **DoS guards** on `/prices/history` (7-day window cap, 10k row limit).
- **Stable sort `(ts, id)`** so `/latest` and `/history` are deterministic when ticks share a timestamp.
- **Three-layer test pyramid:** unit (parsers, validation, pipeline semantics), integration (testcontainers), e2e (live stack + real Binance data).
- **CI on every PR** via GitHub Actions: lint (`golangci-lint v2`), unit + race + coverage, integration with testcontainers.
- **Phased roadmap** documenting the path from MVP to production: hardening → scale (TimescaleDB + continuous aggregates) → multi-exchange.
