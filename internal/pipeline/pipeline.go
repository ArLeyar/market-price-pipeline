package pipeline

import (
	"context"
	"log/slog"

	"github.com/arleyar/market-price-pipeline/internal/domain"
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
	if err := p.db.Save(ctx, tick); err != nil {
		p.log.Error("db save failed", "err", err, "symbol", tick.Symbol)
		return err
	}
	if err := p.cache.SetLatest(ctx, tick); err != nil {
		p.log.Warn("cache set failed", "err", err, "symbol", tick.Symbol)
	}
	if err := p.event.Publish(ctx, tick); err != nil {
		p.log.Warn("event publish failed", "err", err, "symbol", tick.Symbol)
	}
	return nil
}
