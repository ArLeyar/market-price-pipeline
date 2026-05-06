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

## Architecture decisions (locked in)

These are the choices made for the MVP. They are explicit so future contributors know what is *intentional* and what is up for debate.

| # | Decision | Reasoning |
|---|---|---|
| 1 | **Single Go binary, two goroutines** (WS loop + HTTP server) | Lowest moving-parts count. Splitting into ingest / writer / api services is a deployment choice, not an architectural one — fits behind the same interface boundaries when needed. |
| 2 | **Sequential fan-out to three sinks** in `pipeline.HandleTick` (Postgres → Redis → Kafka) | Simpler than goroutine fan-out, easy to reason about and test. Per-sink timeouts (3s / 1s / 2s) prevent a wedged dependency from blocking ingest. |
| 3 | **Postgres is source of truth.** Redis & Kafka are best-effort | A failed cache or event still leaves data persisted. `/health` returns 503 if any dependency is down so the orchestrator can pull the instance out of rotation. |
| 4 | **Kafka is an integration point, not an internal bus** | Service only produces; consumers (analytics, alerting) attach later. Matches the showcase contract — no in-process consumer needed. |
| 5 | **Plain Postgres with `id BIGSERIAL` PK, not composite `(exchange, symbol, ts)`** | Ticks can share a timestamp; a composite PK would reject those as errors. Stable tie-break ordering `(ts, id)` keeps `/latest` and `/history` deterministic. |
| 6 | **TimescaleDB image used from day 1, but plain table in MVP** | The image is wire-compatible with stock Postgres. Switching to a hypertable later is one migration (`create_hypertable`) without touching the Go code or Docker image. |
| 7 | **Resilient WebSocket loop with backoff reset** | Exponential backoff (1s → 30s + jitter); resets to 1s after the first received message so a long session + one drop does not pin the next reconnect at maxBackoff. |
| 8 | **`/prices/history` capped at 7-day window + 10k row limit** | Range-scan on `(symbol, ts DESC)` over an unbounded range can saturate the connection pool. Capping at the handler is one line and removes a public DoS surface. |
| 9 | **Cache-warm on DB fallback in `/prices/latest`** | After Redis restart or TTL expiry, the next request would otherwise hit Postgres until a new tick arrives. Best-effort `SetLatest` after fallback restores the hot path immediately. |
| 10 | **`/health` is a readiness check, not liveness** | It pings dependencies and returns 503 if any is down. Kafka ping is cached for 5s to avoid amplifying load on the broker from a public unauth endpoint. |

## Roadmap

### Phase 1 — MVP (done) ✓

Live Binance WS → Postgres / Redis / Kafka → HTTP API. Health check, validation, integration + e2e tests, single-command compose.

### Phase 2 — production hardening

Targeting: zero data loss, full observability, predictable load handling.

- **Outbox pattern** between Postgres and Kafka. Today an in-flight tick is lost if the process crashes after the DB write but before Kafka publish completes. Add `outbox_events` table written in the same transaction as `prices`, drained by a separate worker.
- **Batched INSERTs.** At >10k tick/s switch to `COPY` or multi-row `INSERT` (100–500 ticks per batch); buffer in a bounded channel between WS handler and DB writer.
- **Prometheus metrics.** `price_tick_total{exchange,symbol}`, `db_save_duration_seconds`, `kafka_publish_total{result}`, `ws_reconnect_total`, `cache_hit_ratio`. Expose `/metrics` endpoint.
- **OpenTelemetry tracing.** Span for each WS message → DB → cache → Kafka. Useful for diagnosing per-sink timeouts.
- **Liveness vs readiness split.** `/livez` (process only) and `/readyz` (dependencies). Avoids spurious restarts on transient Redis/Kafka blips.
- **Structured logging hardening.** Sample WARN spam (raw frame parse failures, sink errors) at high tick rates; log every Nth occurrence with a counter.

### Phase 3 — scale & retention

Targeting: 100+ pairs across multiple exchanges, 2-year retention, sub-second `/history` for long ranges.

- **TimescaleDB activation.** `CREATE EXTENSION timescaledb`, `create_hypertable('prices', 'ts')`, `add_compression_policy('prices', INTERVAL '7 days')`, `add_retention_policy('prices', INTERVAL '2 years')`. Drop-in; the Go code does not change.
- **Continuous aggregates** for downsampled history: `prices_1m`, `prices_5m`, `prices_10m`, `prices_1h`. `/history` accepts `interval=raw|1m|5m|10m`; long ranges read from aggregates instead of raw ticks.
- **Cursor pagination on `/history`.** `?cursor=<ts>_<id>&limit=1000` — `(ts, id)` is already the stable sort key.
- **Connection pool sizing.** `MaxConns` tuned to 4 × `GOMAXPROCS`; metrics on pool saturation.
- **Postgres partition / hypertable maintenance** alerts (compression lag, chunk count).

### Phase 4 — multi-exchange ingestion

Targeting: OKX, Bybit, Coinbase alongside Binance.

- **`PriceStream` interface** in `internal/exchange` — `Run(ctx, symbols, handle TickHandler) error`. Per-exchange package (`internal/exchange/binance`, `internal/exchange/okx`, ...) implements it.
- **Per-exchange goroutines** in `cmd/server/main.go`, each with its own reconnect/backoff loop. Single Kafka topic, key `{exchange}:{symbol}` for partitioning.
- **Symbol normalization.** Each exchange has its own naming (`BTC-USDT` on OKX, `BTCUSDT` on Binance). Normalize at the edge to a canonical form before the pipeline.
- **`/prices/latest?symbol=BTCUSDT&exchange=okx`** already supported by the schema; surface it in the API contract.

### Phase 5 — beyond prices

Optional extensions that fit the same pipeline shape:

- **Order-book snapshots** (`@depth20@100ms` on Binance) → separate topic + table.
- **Trades stream** (`@trade`) for volume / VWAP analytics.
- **Derivatives** — `fstream.binance.com` for futures funding / mark price.
- **Webhook subscriptions:** clients register a URL, we POST significant price moves (>X% / minute).

## Extending the system

### Adding a new exchange

1. Create `internal/exchange/<name>/stream.go` implementing the same shape as `internal/binance/stream.go`: `Run(ctx, handle TickHandler) error` with reconnect/backoff/parser.
2. Add the exchange's WS URL and symbol list to `config.Config` (e.g. `OKX_WS_URL`, `OKX_SYMBOLS`).
3. Wire one more goroutine in `cmd/server/main.go` that calls `stream.Run` with `pipe.HandleTick`.
4. The pipeline, schema, and API need no changes — `exchange` is already a column.

### Adding a new HTTP endpoint

1. Add a method to `internal/http/handlers.go`.
2. If it needs new repository methods, extend `httpapi.Store` (one Go interface, one fake update needed in `handlers_test.go`).
3. Register the route in `internal/http/router.go`.

### Adding observability

1. Add Prometheus client (`prometheus/client_golang`).
2. Register counters / histograms in `cmd/server/main.go` and inject as fields where relevant (`pipeline.Pipeline`, `binance.Stream`, `kafka.Producer`).
3. Mount `/metrics` on the chi router.

### Switching from Redis to a different cache

Implement `httpapi.Cache` and `pipeline.CacheSink`. Both are 3-method interfaces; `internal/storage/redis.go` is a reference implementation.

### Switching the persistence backend

Implement `httpapi.Store` and `pipeline.DBSink`. Subset interfaces are defined in the consumer packages, not in `internal/storage` — the persistence package can change shape without forcing the consumers to adapt.

## Intentionally out of scope for the MVP

- TimescaleDB / hypertables / continuous aggregates (Phase 3)
- In-process Kafka consumer / outbox (Phase 2)
- Batched INSERTs (Phase 2)
- Prometheus / Grafana (Phase 2)
- Splitting ingest / writer / api into separate services (deployment concern, not an architectural change)
