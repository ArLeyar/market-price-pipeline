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

type LatestStore interface {
	GetLatest(ctx context.Context, exchange, symbol string) (domain.Price, error)
}

type CacheStore interface {
	GetLatest(ctx context.Context, exchange, symbol string) (domain.Price, bool, error)
}

type HistoryStore interface {
	GetHistory(ctx context.Context, exchange, symbol string, from, to time.Time, limit int) ([]domain.Price, error)
}

type Pinger interface {
	Ping(ctx context.Context) error
}

type Deps struct {
	DB      LatestStore
	History HistoryStore
	Cache   CacheStore
	DBPing  Pinger
	RDPing  Pinger
	KFPing  Pinger
	Log     *slog.Logger
}

func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(15 * time.Second))

	h := &Handlers{deps: d}
	r.Get("/health", h.Health)
	r.Get("/prices/latest", h.Latest)
	r.Get("/prices/history", h.History)
	return r
}
