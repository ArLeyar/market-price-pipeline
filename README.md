# Market Price Pipeline

Production-like шоукейс-сервис на Go: читает котировки криптопар с Binance по WebSocket, сохраняет историю в Postgres, кэширует latest в Redis, публикует события в Kafka и отдаёт HTTP API.

```
                       ┌──▶ Postgres (история)
Binance WS ──▶ handler─┼──▶ Redis    (latest)
                       └──▶ Kafka    (событие)

                  HTTP API (chi)
                    /health
                    /prices/latest   → Redis (fallback Postgres)
                    /prices/history  → Postgres
```

## Стек

- Go 1.22+
- Postgres 16, Redis 7, Kafka 3.7 (KRaft)
- `pgx/v5`, `redis/go-redis/v9`, `segmentio/kafka-go`, `coder/websocket`, `chi`, `shopspring/decimal`, `slog`
- Тесты: `testcontainers-go`

## Запуск

```bash
cp .env.example .env
docker compose -f deploy/docker-compose.yml --env-file .env up --build
```

Что произойдёт:
1. поднимутся `postgres`, `redis`, `kafka`
2. `migrate` применит `migrations/0001_init.up.sql`
3. `kafka-init` создаст topic `prices.ticks`
4. `server` подключится к Binance, начнёт писать тики

Через ~5 секунд ручки готовы.

## API

### `GET /health`
Dependency / readiness check. Пингует Postgres, Redis, Kafka. `200` если всё up, `503` если хоть что-то down.

```bash
curl -s localhost:8080/health
# {"status":"ok","db":"up","redis":"up","kafka":"up"}
```

### `GET /prices/latest?symbol=BTCUSDT[&exchange=binance]`
Возвращает последнюю цену. Сначала смотрит в Redis-кэш, при miss идёт в Postgres.

```bash
curl -s 'localhost:8080/prices/latest?symbol=BTCUSDT'
# {"exchange":"binance","symbol":"BTCUSDT","ts":"2026-05-06T08:30:12Z","price":"62450.123"}
```

### `GET /prices/history?symbol=BTCUSDT&from=...&to=...[&limit=1000][&exchange=binance]`
История за диапазон, сырые тики. `from` / `to` — RFC3339, `from < to`. `limit` дефолт 1000, max 10000. Сортировка `ts ASC`.

```bash
FROM=$(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ)
TO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -s "localhost:8080/prices/history?symbol=BTCUSDT&from=$FROM&to=$TO&limit=10"
# {"items":[{"exchange":"binance","symbol":"BTCUSDT","ts":"...","price":"..."}, ...], "count": 10}
```

## Конфигурация (`.env`)

| Переменная | Default | Описание |
|---|---|---|
| `HTTP_ADDR` | `:8080` | listen-адрес HTTP API |
| `POSTGRES_DSN` | — | DSN Postgres |
| `REDIS_ADDR` | — | `host:port` Redis |
| `KAFKA_BROKERS` | — | comma-separated брокеры |
| `KAFKA_TOPIC` | `prices.ticks` | topic для событий |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/stream` | WS endpoint |
| `SYMBOLS` | — | comma-separated пары (`BTCUSDT,ETHUSDT,...`) |
| `LATEST_TTL` | `60s` | TTL Redis-кэша |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

## Тесты

**Unit:**
```bash
make test
```
Покрывают парсер Binance bookTicker (happy path + edge cases).

**Integration (изолированный):**
```bash
make integration
```
Поднимает Postgres + Redis + Kafka через `testcontainers-go`, инжектит 3 тика напрямую через `pipeline.HandleTick`. Проверяет:
- `/health` → 200 ok
- `/prices/latest` → последний тик
- `/prices/history` → 3 строки в порядке `ts ASC`
- topic `prices.ticks.test` → 3 события

Требует Docker.

**E2E (на живом compose-стеке + реальная Binance):**
```bash
docker compose -f deploy/docker-compose.yml --env-file .env up -d   # если ещё не поднят
make e2e
```
Не управляет compose, стучится напрямую в `localhost:8080`, `localhost:9094`, `localhost:5432`. Если стек не поднят — тесты `t.Skip` с понятным сообщением. Три независимых теста:

- `TestE2E_API_LiveData` — health, latest свежий (<30s), history sorted ASC и растёт за 3s, negative cases (400/404)
- `TestE2E_Postgres_Growing` — `count(*)` растёт за 3s, оба символа в БД, `max(ts)` свежий
- `TestE2E_Kafka_Publishing` — читает 5 сообщений из topic, валидный JSON, оба символа, цены > 0

Требует поднятый compose и Binance reachable (на Бали — VPN).

## Архитектура и решения

**Один Go-бинарь, две goroutine:** `wsLoop` (Binance WS) и `httpServer` (API). Запускаются из `cmd/server/main.go`, общий graceful shutdown через `signal.NotifyContext`.

**Прямой sequential write в три sink'а** в `pipeline.HandleTick`:
1. **Postgres** — источник правды истории. Если падает — tick failed (ошибка наверх, лог ERROR)
2. **Redis** — best-effort кэш для `/latest`. Ошибка → лог WARN, продолжаем
3. **Kafka** — best-effort событие для внешних потребителей. Ошибка → лог WARN, продолжаем

`/health` агрегирует состояние зависимостей: degraded → 503, чтобы оркестратор мог снять с балансировки.

**Устойчивый WS-loop:** reconnect с экспоненциальным backoff (1s → 30s + jitter), context cancellation, read deadline 60s. Без этого demo падает от первого сетевого hiccup.

**Kafka здесь как точка интеграции** для будущих внешних потребителей (аналитика, alerting). Сам сервис не консьюмит — соответствует требованию задачи.

**Postgres без Timescale в MVP.** Schema:
```sql
prices(id BIGSERIAL PK, exchange, symbol, ts, price NUMERIC(38,18))
+ index (symbol, ts DESC)
+ index (exchange, symbol, ts DESC)
```
PK по `id`, не `(exchange, symbol, ts)` — тики могут прилететь с одинаковым timestamp, PK по нему отбрасывал бы дубли как ошибку.

## Что бы я добавил для прода

- **TimescaleDB:** `timescale/timescaledb:pg16` образ, `create_hypertable('prices', 'ts')`, continuous aggregates `prices_1m`/`prices_5m`/`prices_10m`, retention policy 2 года, columnar compression на старых чанках. Образ совместим с текущим кодом, миграция включается одной командой
- **Kafka-consumer для durability:** сейчас при крэше теряем in-flight тики (нет outbox). В проде — либо outbox-паттерн, либо writer как отдельный consumer, читающий из Kafka и пишущий в БД с at-least-once
- **Батчинг INSERT'ов:** при росте до 10k tick/s — `COPY` или `INSERT ... VALUES (...), (...), ...` батчами по 100-500
- **Cursor-пагинация `/history`:** при сырых тиках 100ms-частоты сутки = ~864k строк на пару, нужен cursor по `(ts, id)`
- **Несколько CEX:** OKX, Bybit через интерфейс `PriceStream`, fan-in в один Kafka topic с ключом `{exchange}:{symbol}`
- **Observability:** Prometheus метрики (`price_tick_total`, `db_save_duration_seconds`, `kafka_publish_total`, `ws_reconnect_total`), tracing (OpenTelemetry), Grafana-дашборд
- **Liveness vs readiness:** разнести `/health` на `/livez` (только сам процесс) и `/readyz` (зависимости)

## Сознательно НЕ делаем в MVP

- Timescale / hypertable / continuous aggregates
- Kafka-consumer внутри сервиса
- Батчинг INSERT'ов (1k/s одиночек Postgres переварит)
- Outbox / ретраи
- Prometheus / Grafana
- Несколько отдельных сервисов под ingest/writer/api
