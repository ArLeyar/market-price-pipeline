package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

// Store is the persistent backend for prices and serves both the latest and
// the history endpoints. /latest reads from cache first and falls back here.
type Store interface {
	GetLatest(ctx context.Context, exchange, symbol string) (domain.Price, error)
	GetHistory(ctx context.Context, exchange, symbol string, from, to time.Time, limit int) ([]domain.Price, error)
	Ping(ctx context.Context) error
}

// Cache is the hot-path latest-price source. Failures fall through to Store.
type Cache interface {
	GetLatest(ctx context.Context, exchange, symbol string) (domain.Price, bool, error)
	SetLatest(ctx context.Context, p domain.Price) error
	Ping(ctx context.Context) error
}

// EventBus exposes only what /health needs from the event-publishing side.
type EventBus interface {
	Ping(ctx context.Context) error
}

func NewRouter(store Store, cache Cache, events EventBus, log *slog.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(15 * time.Second))

	h := &Handlers{store: store, cache: cache, events: events, log: log}
	r.Get("/health", h.Health)
	r.Get("/prices/latest", h.Latest)
	r.Get("/prices/history", h.History)
	return r
}
