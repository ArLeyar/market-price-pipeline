//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/arleyar/market-price-pipeline/internal/domain"
	httpapi "github.com/arleyar/market-price-pipeline/internal/http"
	mykafka "github.com/arleyar/market-price-pipeline/internal/kafka"
	"github.com/arleyar/market-price-pipeline/internal/pipeline"
	"github.com/arleyar/market-price-pipeline/internal/storage"
)

const topic = "prices.ticks.test"

func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// --- Postgres ---
	pgC, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("prices"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	pgDSN, err := pgC.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	applyMigrations(t, ctx, pgDSN)

	// --- Redis ---
	rdC, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdC.Terminate(ctx) })
	rdEndpoint, err := rdC.Endpoint(ctx, "")
	require.NoError(t, err)

	// --- Kafka ---
	kfC, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kfC.Terminate(ctx) })
	kfBrokers, err := kfC.Brokers(ctx)
	require.NoError(t, err)

	createTopic(t, kfBrokers[0], topic)

	// --- Wire ---
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	pg, err := storage.NewPostgres(ctx, pgDSN)
	require.NoError(t, err)
	t.Cleanup(pg.Close)

	rd := storage.NewRedis(rdEndpoint, time.Minute)
	t.Cleanup(func() { _ = rd.Close() })

	kp := mykafka.NewProducer(kfBrokers, topic)
	t.Cleanup(func() { _ = kp.Close() })

	pipe := pipeline.New(pg, rd, kp, log)

	router := httpapi.NewRouter(httpapi.Deps{
		DB:      pg,
		History: pg,
		Cache:   rd,
		DBPing:  pg,
		RDPing:  rd,
		KFPing:  kp,
		Log:     log,
	})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	// --- Inject 3 ticks ---
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	ticks := []domain.Price{
		{Exchange: "binance", Symbol: "BTCUSDT", TS: base, Price: decimal.RequireFromString("60000.5")},
		{Exchange: "binance", Symbol: "BTCUSDT", TS: base.Add(time.Second), Price: decimal.RequireFromString("60001")},
		{Exchange: "binance", Symbol: "BTCUSDT", TS: base.Add(2 * time.Second), Price: decimal.RequireFromString("60002.25")},
	}
	for _, tk := range ticks {
		require.NoError(t, pipe.HandleTick(ctx, tk))
	}

	// --- /health ---
	resp, err := http.Get(srv.URL + "/health")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// --- /prices/latest ---
	resp, err = http.Get(srv.URL + "/prices/latest?symbol=BTCUSDT")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var latest map[string]any
	require.NoError(t, json.Unmarshal(body, &latest))
	require.Equal(t, "BTCUSDT", latest["symbol"])
	require.Equal(t, "60002.25", latest["price"])

	// --- /prices/history ---
	from := base.Add(-time.Second).Format(time.RFC3339)
	to := base.Add(10 * time.Second).Format(time.RFC3339)
	histURL := srv.URL + "/prices/history?symbol=BTCUSDT&from=" + url.QueryEscape(from) + "&to=" + url.QueryEscape(to)
	resp, err = http.Get(histURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var hist struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	require.NoError(t, json.Unmarshal(body, &hist))
	require.Equal(t, 3, hist.Count)
	require.Equal(t, "60000.5", hist.Items[0]["price"])
	require.Equal(t, "60002.25", hist.Items[2]["price"])

	// --- Kafka topic should have 3 events ---
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  kfBrokers,
		Topic:    topic,
		GroupID:  "integration-test",
		MinBytes: 1,
		MaxBytes: 1 << 20,
	})
	t.Cleanup(func() { _ = reader.Close() })

	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	got := 0
	for got < 3 {
		m, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read kafka: %v (got %d)", err, got)
		}
		var p domain.Price
		require.NoError(t, json.Unmarshal(m.Value, &p))
		require.Equal(t, "BTCUSDT", p.Symbol)
		got++
	}
	require.Equal(t, 3, got)
}

func applyMigrations(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	root, err := findRepoRoot()
	require.NoError(t, err)
	upFile := filepath.Join(root, "migrations", "0001_init.up.sql")
	sql, err := os.ReadFile(upFile)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(sql))
	require.NoError(t, err)
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		dir = filepath.Dir(dir)
	}
	return "", fmt.Errorf("go.mod not found")
}

func createTopic(t *testing.T, broker, name string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", broker)
	require.NoError(t, err)
	defer conn.Close()
	controller, err := conn.Controller()
	require.NoError(t, err)
	ctrlConn, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	require.NoError(t, err)
	defer ctrlConn.Close()
	require.NoError(t, ctrlConn.CreateTopics(kafkago.TopicConfig{
		Topic:             name,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}))
}
