# Market Price Pipeline

Production-like showcase service in Go: ingests crypto-pair prices from Binance over WebSocket, persists history to Postgres, caches the latest tick in Redis, publishes events to Kafka, and exposes an HTTP API.

```
                       ┌──▶ Postgres (history)
Binance WS ──▶ handler─┼──▶ Redis    (latest)
                       └──▶ Kafka    (event)

                  HTTP API (chi)
                    /health
                    /prices/latest   → Redis (fallback Postgres)
                    /prices/history  → Postgres
```

## Stack

- Go 1.22+
- Postgres 16, Redis 7, Kafka 3.7 (KRaft)
- `pgx/v5`, `redis/go-redis/v9`, `segmentio/kafka-go`, `coder/websocket`, `chi`, `shopspring/decimal`, `slog`
- Tests: `testcontainers-go`

## Quick start

```bash
cp .env.example .env
docker compose -f deploy/docker-compose.yml --env-file .env up --build
```

What happens:
1. `postgres`, `redis`, `kafka` come up healthy
2. `migrate` applies `migrations/0001_init.up.sql`
3. `kafka-init` creates topic `prices.ticks`
4. `server` connects to Binance and starts writing ticks

The API is reachable on `:8080` once the server starts.

## API

### `GET /health`
Dependency / readiness check. Pings Postgres, Redis, and Kafka. Returns `200` if all are up, `503` if any is down.

```bash
curl -s localhost:8080/health
# {"status":"ok","db":"up","redis":"up","kafka":"up"}
```

### `GET /prices/latest?symbol=BTCUSDT[&exchange=binance]`
Latest price. Reads from Redis first, falls back to Postgres on miss; warms the cache on fallback.

```bash
curl -s 'localhost:8080/prices/latest?symbol=BTCUSDT'
# {"exchange":"binance","symbol":"BTCUSDT","ts":"2026-05-06T08:30:12Z","price":"62450.123"}
```

### `GET /prices/history?symbol=BTCUSDT&from=...&to=...[&limit=1000][&exchange=binance]`
Raw tick history for the requested range. `from` / `to` are RFC3339, `from < to`. `limit` defaults to 1000, max 10000. The total range is capped at 7 days. Sorted by `ts ASC`.

```bash
FROM=$(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ)
TO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -s "localhost:8080/prices/history?symbol=BTCUSDT&from=$FROM&to=$TO&limit=10"
# {"items":[{"exchange":"binance","symbol":"BTCUSDT","ts":"...","price":"..."}, ...], "count": 10}
```

## Configuration (`.env`)

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDR` | `:8080` | API listen address |
| `POSTGRES_DSN` | — | Postgres DSN |
| `REDIS_ADDR` | — | Redis `host:port` |
| `KAFKA_BROKERS` | — | comma-separated brokers |
| `KAFKA_TOPIC` | `prices.ticks` | event topic |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/stream` | WebSocket endpoint |
| `SYMBOLS` | — | comma-separated pairs (`BTCUSDT,ETHUSDT,...`) |
| `LATEST_TTL` | `60s` | Redis cache TTL |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

## Tests

**Unit:**
```bash
make test
```
Covers: Binance bookTicker parser; pipeline failure semantics + per-sink timeouts; HTTP handlers (validation matrix, cache hit/miss/error fallbacks, `ErrNotFound`→404, DB error→500, limit clamp, 7-day range cap, cache warm); config trim/empty/validation.

**Integration (isolated, testcontainers):**
```bash
make integration
```
Spins up Postgres + Redis + Kafka via `testcontainers-go`, injects 3 ticks directly through `pipeline.HandleTick`, asserts:
- `/health` → 200 ok
- `/prices/latest` → newest tick
- `/prices/history` → 3 rows ordered by `ts ASC`
- topic `prices.ticks.test` → 3 events

Requires Docker.

**E2E (against the live compose stack + real Binance):**
```bash
docker compose -f deploy/docker-compose.yml --env-file .env up -d   # if not yet running
make e2e
```
Does not manage compose; talks directly to `localhost:8080`, `localhost:9094`, `localhost:5432`. If the stack is not up, tests `t.Skip` with a clear message. Three independent tests:

- `TestE2E_API_LiveData` — health, fresh latest (<30s), history sorted ASC, count grows over 3s, negative cases (400/404)
- `TestE2E_Postgres_Growing` — `count(*)` grows over 3s, both symbols present, `max(ts)` is fresh
- `TestE2E_Kafka_Publishing` — reads 5 messages from the topic, valid JSON, both symbols, prices > 0

Requires the compose stack running and Binance reachable.

## Architecture & decisions

**Single Go binary, two goroutines:** `wsLoop` (Binance WS) and `httpServer` (API). Wired in `cmd/server/main.go` with a shared graceful shutdown via `signal.NotifyContext`. `wg.Wait()` is bounded with a 15s timeout so a stuck WS read cannot outlive the SIGTERM grace period.

**Sequential write to three sinks** in `pipeline.HandleTick`, each wrapped in a per-sink timeout:
1. **Postgres** — source of truth. Failure aborts the tick (logged at ERROR).
2. **Redis** — best-effort latest cache. Failures logged at WARN, ignored.
3. **Kafka** — best-effort event for downstream consumers. Failures logged at WARN, ignored.

`/health` aggregates dependency state and returns 503 if any dependency is down so an orchestrator can pull the instance out of rotation.

**Resilient WebSocket loop:** reconnect with exponential backoff (1s → 30s, jittered), context cancellation, 60s read deadline. Backoff resets to 1s after the first message in a session — without that reset, a long-lived session followed by a single drop would pin the next reconnect at maxBackoff forever.

**Kafka here is an integration point**, not an internal bus. The service produces only; downstream consumers (analytics, alerting) attach later. Matches the showcase requirement.

**Postgres without TimescaleDB in the MVP.** Schema:
```sql
prices(id BIGSERIAL PK, exchange, symbol, ts, price NUMERIC(38,18))
+ index (symbol, ts DESC)
+ index (exchange, symbol, ts DESC)
```
PK on `id` (not `(exchange, symbol, ts)`) because ticks can share a timestamp and a composite PK on `ts` would reject duplicates as errors. `/latest` and `/history` use stable tie-break ordering `(ts, id)`.

## What I'd add for production

- **TimescaleDB:** swap to `timescale/timescaledb:pg16` image, `create_hypertable('prices', 'ts')`, continuous aggregates `prices_1m` / `prices_5m` / `prices_10m`, 2-year retention policy, columnar compression on old chunks. Image is wire-compatible with the current code; the migration is a single command.
- **Kafka consumer for durability:** today an in-flight tick is lost on a crash (no outbox). In production: outbox pattern, or move the writer behind Kafka with at-least-once semantics.
- **Batched INSERTs:** at 10k tick/s use `COPY` or multi-row `INSERT` batches (100–500).
- **Cursor-based pagination for `/history`:** at 100ms ticks, a day per pair is ~864k rows; need `(ts, id)` cursor.
- **Multiple CEX:** OKX, Bybit behind a `PriceStream` interface; fan-in into one Kafka topic keyed by `{exchange}:{symbol}`.
- **Observability:** Prometheus metrics (`price_tick_total`, `db_save_duration_seconds`, `kafka_publish_total`, `ws_reconnect_total`), tracing (OpenTelemetry), Grafana dashboard.
- **Liveness vs readiness:** split `/health` into `/livez` (process only) and `/readyz` (dependencies).

## Intentionally out of scope for the MVP

- TimescaleDB / hypertables / continuous aggregates
- In-process Kafka consumer
- Batched INSERTs (a single Postgres handles 1k tick/s comfortably)
- Outbox / retries
- Prometheus / Grafana
- Splitting ingest / writer / api into separate services
