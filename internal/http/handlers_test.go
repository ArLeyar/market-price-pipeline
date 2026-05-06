package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/arleyar/market-price-pipeline/internal/domain"
	"github.com/arleyar/market-price-pipeline/internal/storage"
)

// --- fakes ---

type fakeStore struct {
	latestPrice domain.Price
	latestErr   error
	historyRows []domain.Price
	historyErr  error
	pingErr     error
	getCalls    int32
	histCalls   int32
}

func (f *fakeStore) GetLatest(ctx context.Context, _, _ string) (domain.Price, error) {
	atomic.AddInt32(&f.getCalls, 1)
	return f.latestPrice, f.latestErr
}
func (f *fakeStore) GetHistory(ctx context.Context, _, _ string, _, _ time.Time, _ int) ([]domain.Price, error) {
	atomic.AddInt32(&f.histCalls, 1)
	return f.historyRows, f.historyErr
}
func (f *fakeStore) Ping(ctx context.Context) error { return f.pingErr }

type fakeCache struct {
	latestPrice domain.Price
	latestHit   bool
	latestErr   error
	pingErr     error
	getCalls    int32
	setCalls    int32
}

func (f *fakeCache) GetLatest(ctx context.Context, _, _ string) (domain.Price, bool, error) {
	atomic.AddInt32(&f.getCalls, 1)
	return f.latestPrice, f.latestHit, f.latestErr
}
func (f *fakeCache) SetLatest(ctx context.Context, _ domain.Price) error {
	atomic.AddInt32(&f.setCalls, 1)
	return nil
}
func (f *fakeCache) Ping(ctx context.Context) error { return f.pingErr }

type fakeEvents struct{ pingErr error }

func (f *fakeEvents) Ping(ctx context.Context) error { return f.pingErr }

func quietLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func newServer(store *fakeStore, cache *fakeCache, events *fakeEvents) *httptest.Server {
	return httptest.NewServer(NewRouter(store, cache, events, quietLog()))
}

func samplePrice() domain.Price {
	return domain.Price{
		Exchange: "binance",
		Symbol:   "BTCUSDT",
		TS:       time.Date(2026, 5, 6, 9, 0, 0, 0, time.UTC),
		Price:    decimal.RequireFromString("60123.45"),
	}
}

// --- /health ---

func TestHealth(t *testing.T) {
	cases := []struct {
		name           string
		dbErr, rdErr   error
		kfErr          error
		wantStatusCode int
		wantBodyStatus string
	}{
		{"all up", nil, nil, nil, http.StatusOK, "ok"},
		{"db down", errors.New("x"), nil, nil, http.StatusServiceUnavailable, "degraded"},
		{"redis down", nil, errors.New("x"), nil, http.StatusServiceUnavailable, "degraded"},
		{"kafka down", nil, nil, errors.New("x"), http.StatusServiceUnavailable, "degraded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(
				&fakeStore{pingErr: tc.dbErr},
				&fakeCache{pingErr: tc.rdErr},
				&fakeEvents{pingErr: tc.kfErr},
			)
			defer srv.Close()
			resp, err := http.Get(srv.URL + "/health")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatusCode {
				t.Fatalf("status: got %d, want %d", resp.StatusCode, tc.wantStatusCode)
			}
			var body map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&body)
			if body["status"] != tc.wantBodyStatus {
				t.Errorf("status field: got %v, want %s", body["status"], tc.wantBodyStatus)
			}
		})
	}
}

// --- /prices/latest ---

func TestLatest_CacheHit(t *testing.T) {
	store := &fakeStore{}
	cache := &fakeCache{latestPrice: samplePrice(), latestHit: true}
	srv := newServer(store, cache, &fakeEvents{})
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/prices/latest?symbol=BTCUSDT")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&store.getCalls); got != 0 {
		t.Errorf("DB must NOT be called on cache hit; got %d", got)
	}
	if got := atomic.LoadInt32(&cache.setCalls); got != 0 {
		t.Errorf("cache.SetLatest must not be called on cache hit; got %d", got)
	}
}

func TestLatest_CacheMissFallsBackToDBAndWarmsCache(t *testing.T) {
	store := &fakeStore{latestPrice: samplePrice()}
	cache := &fakeCache{latestHit: false}
	srv := newServer(store, cache, &fakeEvents{})
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/prices/latest?symbol=BTCUSDT")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&store.getCalls); got != 1 {
		t.Errorf("DB must be called once on cache miss; got %d", got)
	}
	// Cache warm should fire on successful DB fallback.
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&cache.setCalls) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&cache.setCalls); got != 1 {
		t.Errorf("cache must be warmed on DB fallback; got %d set calls", got)
	}
}

func TestLatest_CacheErrorFallsBackToDB(t *testing.T) {
	store := &fakeStore{latestPrice: samplePrice()}
	cache := &fakeCache{latestErr: errors.New("redis dropped")}
	srv := newServer(store, cache, &fakeEvents{})
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/prices/latest?symbol=BTCUSDT")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&store.getCalls); got != 1 {
		t.Errorf("DB must be called when cache errors; got %d", got)
	}
}

func TestLatest_NotFound(t *testing.T) {
	store := &fakeStore{latestErr: storage.ErrNotFound}
	srv := newServer(store, &fakeCache{}, &fakeEvents{})
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/prices/latest?symbol=NOPE")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}

func TestLatest_DBError(t *testing.T) {
	store := &fakeStore{latestErr: errors.New("boom")}
	srv := newServer(store, &fakeCache{}, &fakeEvents{})
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/prices/latest?symbol=BTCUSDT")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", resp.StatusCode)
	}
}

func TestLatest_MissingSymbol(t *testing.T) {
	srv := newServer(&fakeStore{}, &fakeCache{}, &fakeEvents{})
	defer srv.Close()
	resp, _ := http.Get(srv.URL + "/prices/latest")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

// --- /prices/history ---

func TestHistory_Validation(t *testing.T) {
	now := time.Now().UTC()
	rfc := func(t time.Time) string { return url.QueryEscape(t.Format(time.RFC3339)) }

	cases := []struct {
		name     string
		query    string
		wantCode int
	}{
		{"missing symbol", "from=" + rfc(now.Add(-time.Hour)) + "&to=" + rfc(now), http.StatusBadRequest},
		{"missing from", "symbol=BTCUSDT&to=" + rfc(now), http.StatusBadRequest},
		{"missing to", "symbol=BTCUSDT&from=" + rfc(now.Add(-time.Hour)), http.StatusBadRequest},
		{"garbage from", "symbol=BTCUSDT&from=garbage&to=" + rfc(now), http.StatusBadRequest},
		{"garbage to", "symbol=BTCUSDT&from=" + rfc(now.Add(-time.Hour)) + "&to=alsogarbage", http.StatusBadRequest},
		{"from >= to", "symbol=BTCUSDT&from=" + rfc(now) + "&to=" + rfc(now.Add(-time.Hour)), http.StatusBadRequest},
		{"range exceeds 7 days", "symbol=BTCUSDT&from=" + rfc(now.Add(-30*24*time.Hour)) + "&to=" + rfc(now), http.StatusBadRequest},
		{"negative limit", "symbol=BTCUSDT&from=" + rfc(now.Add(-time.Hour)) + "&to=" + rfc(now) + "&limit=-1", http.StatusBadRequest},
		{"non-numeric limit", "symbol=BTCUSDT&from=" + rfc(now.Add(-time.Hour)) + "&to=" + rfc(now) + "&limit=abc", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			srv := newServer(store, &fakeCache{}, &fakeEvents{})
			defer srv.Close()
			resp, _ := http.Get(srv.URL + "/prices/history?" + tc.query)
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantCode {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status: got %d, want %d; body=%s", resp.StatusCode, tc.wantCode, string(body))
			}
			if atomic.LoadInt32(&store.histCalls) != 0 {
				t.Errorf("validation failure must not reach DB")
			}
		})
	}
}

func TestHistory_HappyPath(t *testing.T) {
	now := time.Now().UTC()
	rows := []domain.Price{
		{Exchange: "binance", Symbol: "BTCUSDT", TS: now.Add(-2 * time.Second), Price: decimal.RequireFromString("60000")},
		{Exchange: "binance", Symbol: "BTCUSDT", TS: now.Add(-1 * time.Second), Price: decimal.RequireFromString("60001")},
	}
	store := &fakeStore{historyRows: rows}
	srv := newServer(store, &fakeCache{}, &fakeEvents{})
	defer srv.Close()

	q := url.Values{
		"symbol": []string{"BTCUSDT"},
		"from":   []string{now.Add(-10 * time.Second).Format(time.RFC3339)},
		"to":     []string{now.Format(time.RFC3339)},
	}
	resp, _ := http.Get(srv.URL + "/prices/history?" + q.Encode())
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d body=%s", resp.StatusCode, string(body))
	}
	var out struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Count != 2 || len(out.Items) != 2 {
		t.Fatalf("count/items: %+v", out)
	}
	if !strings.HasPrefix(out.Items[0]["price"].(string), "60000") {
		t.Errorf("first price wrong: %v", out.Items[0])
	}
}

func TestHistory_LimitClampedToMax(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{}
	srv := newServer(store, &fakeCache{}, &fakeEvents{})
	defer srv.Close()

	// limit=99999 must be clamped to 10000 and accepted (not 400)
	q := "symbol=BTCUSDT&from=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339)) +
		"&to=" + url.QueryEscape(now.Format(time.RFC3339)) + "&limit=99999"
	resp, _ := http.Get(srv.URL + "/prices/history?" + q)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&store.histCalls) != 1 {
		t.Errorf("DB must be called once; got %d", atomic.LoadInt32(&store.histCalls))
	}
}
