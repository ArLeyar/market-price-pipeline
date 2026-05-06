//go:build e2e

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

// --- env helpers ---

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func apiURL() string         { return env("E2E_API_URL", "http://localhost:8080") }
func pgDSN() string          { return env("E2E_PG_DSN", "postgres://postgres:postgres@localhost:5432/prices?sslmode=disable") }
func kafkaBrokers() []string { return strings.Split(env("E2E_KAFKA_BROKERS", "localhost:9094"), ",") }
func kafkaTopic() string     { return env("E2E_KAFKA_TOPIC", "prices.ticks") }

// freshness window for "live" data assertions
const freshnessWindow = 30 * time.Second

// --- TestE2E_API_LiveData ---

func TestE2E_API_LiveData(t *testing.T) {
	skipIfNoStack(t)

	// 1. /health
	healthBody := getJSON(t, apiURL()+"/health", http.StatusOK)
	require.Equal(t, "ok", healthBody["status"], "health: %v", healthBody)
	require.Equal(t, "up", healthBody["db"])
	require.Equal(t, "up", healthBody["redis"])
	require.Equal(t, "up", healthBody["kafka"])

	// 2. wait for first tick (Binance handshake may need a few seconds)
	if !waitForFirstTick(t, "BTCUSDT", 30*time.Second) {
		t.Skip("no live tick within 30s — check Binance reachable / VPN")
	}

	// 3. latest BTCUSDT + ETHUSDT
	for _, sym := range []string{"BTCUSDT", "ETHUSDT"} {
		body := getJSON(t, apiURL()+"/prices/latest?symbol="+sym, http.StatusOK)
		require.Equal(t, sym, body["symbol"])
		require.Equal(t, "binance", body["exchange"])

		price, err := decimal.NewFromString(body["price"].(string))
		require.NoError(t, err)
		require.True(t, price.Sign() > 0, "price must be > 0, got %s", price)

		ts, err := time.Parse(time.RFC3339Nano, body["ts"].(string))
		require.NoError(t, err)
		require.Less(t, time.Since(ts), freshnessWindow, "ts must be fresh, got %s (now=%s)", ts, time.Now())
	}

	// 4. /prices/history — last 30s, count>0, sorted ASC, all prices > 0
	hist1 := getHistory(t, "BTCUSDT", 30*time.Second)
	require.NotEmpty(t, hist1.Items, "history must contain ticks")
	require.True(t, sortedASC(hist1.Items), "history must be sorted by ts ASC")
	for _, it := range hist1.Items {
		p, err := decimal.NewFromString(it.Price)
		require.NoError(t, err)
		require.True(t, p.Sign() > 0)
	}

	// 5. live stream: count grows after sleep
	time.Sleep(3 * time.Second)
	hist2 := getHistory(t, "BTCUSDT", 30*time.Second)
	require.Greater(t, hist2.Count, hist1.Count, "history count must grow (stream is live); was %d, now %d", hist1.Count, hist2.Count)

	// 6. negative cases
	t.Run("missing symbol -> 400", func(t *testing.T) {
		require.Equal(t, http.StatusBadRequest, getStatus(t, apiURL()+"/prices/latest"))
	})
	t.Run("unknown symbol -> 404", func(t *testing.T) {
		require.Equal(t, http.StatusNotFound, getStatus(t, apiURL()+"/prices/latest?symbol=NOPECOIN"))
	})
	t.Run("from > to -> 400", func(t *testing.T) {
		now := time.Now().UTC()
		u := fmt.Sprintf("%s/prices/history?symbol=BTCUSDT&from=%s&to=%s",
			apiURL(),
			url.QueryEscape(now.Format(time.RFC3339)),
			url.QueryEscape(now.Add(-time.Minute).Format(time.RFC3339)),
		)
		require.Equal(t, http.StatusBadRequest, getStatus(t, u))
	})
	t.Run("garbage dates -> 400", func(t *testing.T) {
		u := apiURL() + "/prices/history?symbol=BTCUSDT&from=garbage&to=alsogarbage"
		require.Equal(t, http.StatusBadRequest, getStatus(t, u))
	})
	t.Run("invalid limit -> 400", func(t *testing.T) {
		now := time.Now().UTC()
		u := fmt.Sprintf("%s/prices/history?symbol=BTCUSDT&from=%s&to=%s&limit=-5",
			apiURL(),
			url.QueryEscape(now.Add(-time.Minute).Format(time.RFC3339)),
			url.QueryEscape(now.Format(time.RFC3339)),
		)
		require.Equal(t, http.StatusBadRequest, getStatus(t, u))
	})
}

// --- TestE2E_Postgres_Growing ---

func TestE2E_Postgres_Growing(t *testing.T) {
	skipIfNoStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, pgDSN())
	if err != nil {
		t.Skipf("postgres not reachable at %s: %v — did you `docker compose up`?", pgDSN(), err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres ping failed: %v", err)
	}

	// First reading
	var c1 int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM prices`).Scan(&c1))
	if c1 == 0 {
		// Maybe stack just started — give it a moment
		time.Sleep(5 * time.Second)
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM prices`).Scan(&c1))
		if c1 == 0 {
			t.Skip("no rows yet — Binance may be unreachable")
		}
	}

	// Second reading
	time.Sleep(3 * time.Second)
	var c2 int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM prices`).Scan(&c2))
	require.Greater(t, c2, c1, "row count must grow (live stream); was %d, now %d", c1, c2)

	// Both symbols present
	rows, err := pool.Query(ctx, `SELECT DISTINCT symbol FROM prices`)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		got[s] = true
	}
	require.True(t, got["BTCUSDT"], "BTCUSDT must be in DB; got %v", got)
	require.True(t, got["ETHUSDT"], "ETHUSDT must be in DB; got %v", got)

	// Latest ts is fresh
	var maxTS time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(ts) FROM prices`).Scan(&maxTS))
	require.Less(t, time.Since(maxTS), freshnessWindow, "max(ts) must be fresh, got %s", maxTS)
}

// --- TestE2E_Kafka_Publishing ---

func TestE2E_Kafka_Publishing(t *testing.T) {
	skipIfNoStack(t)

	// Probe broker reachability before constructing reader
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer probeCancel()
	dialer := &kafkago.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(probeCtx, "tcp", kafkaBrokers()[0])
	if err != nil {
		t.Skipf("kafka not reachable at %s: %v — did you `docker compose up`?", kafkaBrokers()[0], err)
	}
	conn.Close()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     kafkaBrokers(),
		Topic:       kafkaTopic(),
		GroupID:     fmt.Sprintf("e2e-%d", time.Now().UnixNano()),
		StartOffset: kafkago.LastOffset,
		MinBytes:    1,
		MaxBytes:    1 << 20,
	})
	defer reader.Close()

	const want = 5
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := 0
	validSymbols := map[string]bool{"BTCUSDT": true, "ETHUSDT": true}
	for got < want {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("kafka read failed after %d messages: %v", got, err)
		}
		var p domain.Price
		require.NoError(t, json.Unmarshal(m.Value, &p), "raw=%s", string(m.Value))
		require.True(t, validSymbols[p.Symbol], "unexpected symbol: %s", p.Symbol)
		require.Equal(t, "binance", p.Exchange)
		require.True(t, p.Price.Sign() > 0, "price must be > 0; got %s", p.Price)
		require.Less(t, time.Since(p.TS), freshnessWindow, "tick ts must be fresh, got %s", p.TS)
		got++
	}
	require.Equal(t, want, got)
}

// --- helpers ---

func skipIfNoStack(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(apiURL() + "/health")
	if err != nil {
		t.Skipf("server not reachable at %s: %v — did you `docker compose -f deploy/docker-compose.yml --env-file .env up -d`?", apiURL(), err)
	}
	resp.Body.Close()
}

func getJSON(t *testing.T, urlStr string, wantStatus int) map[string]any {
	t.Helper()
	resp, err := http.Get(urlStr)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, wantStatus, resp.StatusCode, "url=%s body=%s", urlStr, string(body))
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func getStatus(t *testing.T, urlStr string) int {
	t.Helper()
	resp, err := http.Get(urlStr)
	require.NoError(t, err)
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

type historyItem struct {
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
	TS       string `json:"ts"`
	Price    string `json:"price"`
}

type historyResp struct {
	Items []historyItem `json:"items"`
	Count int           `json:"count"`
}

func getHistory(t *testing.T, symbol string, window time.Duration) historyResp {
	t.Helper()
	now := time.Now().UTC()
	from := now.Add(-window).Format(time.RFC3339)
	to := now.Format(time.RFC3339)
	u := fmt.Sprintf("%s/prices/history?symbol=%s&from=%s&to=%s&limit=10000",
		apiURL(), symbol, url.QueryEscape(from), url.QueryEscape(to))
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))
	var h historyResp
	require.NoError(t, json.Unmarshal(body, &h))
	return h
}

func sortedASC(items []historyItem) bool {
	var prev time.Time
	for i, it := range items {
		ts, err := time.Parse(time.RFC3339Nano, it.TS)
		if err != nil {
			return false
		}
		if i > 0 && ts.Before(prev) {
			return false
		}
		prev = ts
	}
	return true
}

func waitForFirstTick(t *testing.T, symbol string, max time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		resp, err := http.Get(apiURL() + "/prices/latest?symbol=" + symbol)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(time.Second)
	}
	return false
}
