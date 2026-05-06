package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

// Per-sink timeouts. WS-loop ctx has no deadline; without these a stuck
// dependency would block the entire ingest path indefinitely.
const (
	dbTimeout    = 3 * time.Second
	cacheTimeout = 1 * time.Second
	eventTimeout = 2 * time.Second
)

type DBSink interface {
	Save(ctx context.Context, p domain.Price) error
}

type CacheSink interface {
	SetLatest(ctx context.Context, p domain.Price) error
}

type EventSink interface {
	Publish(ctx context.Context, p domain.Price) error
}

type Pipeline struct {
	db    DBSink
	cache CacheSink
	event EventSink
	log   *slog.Logger
}

func New(db DBSink, cache CacheSink, event EventSink, log *slog.Logger) *Pipeline {
	return &Pipeline{db: db, cache: cache, event: event, log: log}
}

// HandleTick persists a tick and best-effort updates cache and event log.
// Postgres is the source of truth: failure here aborts the tick.
// Redis and Kafka failures are logged as WARN and ignored.
func (p *Pipeline) HandleTick(ctx context.Context, tick domain.Price) error {
	dbCtx, dbCancel := context.WithTimeout(ctx, dbTimeout)
	defer dbCancel()
	if err := p.db.Save(dbCtx, tick); err != nil {
		p.log.Error("db save failed", "err", err, "symbol", tick.Symbol)
		return err
	}

	cacheCtx, cacheCancel := context.WithTimeout(ctx, cacheTimeout)
	defer cacheCancel()
	if err := p.cache.SetLatest(cacheCtx, tick); err != nil {
		p.log.Warn("cache set failed", "err", err, "symbol", tick.Symbol)
	}

	eventCtx, eventCancel := context.WithTimeout(ctx, eventTimeout)
	defer eventCancel()
	if err := p.event.Publish(eventCtx, tick); err != nil {
		p.log.Warn("event publish failed", "err", err, "symbol", tick.Symbol)
	}
	return nil
}
