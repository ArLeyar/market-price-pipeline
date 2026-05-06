package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

var ErrNotFound = errors.New("price not found")

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *Postgres) Save(ctx context.Context, price domain.Price) error {
	const q = `INSERT INTO prices (exchange, symbol, ts, price) VALUES ($1, $2, $3, $4)`
	_, err := p.pool.Exec(ctx, q, price.Exchange, price.Symbol, price.TS, price.Price)
	if err != nil {
		return fmt.Errorf("insert price: %w", err)
	}
	return nil
}

func (p *Postgres) GetLatest(ctx context.Context, exchange, symbol string) (domain.Price, error) {
	// Stable tie-break by id: with 100ms ticks the same ts can appear twice;
	// without it Postgres may pick either row, masking the truly latest insert.
	const q = `
		SELECT exchange, symbol, ts, price
		FROM prices
		WHERE exchange = $1 AND symbol = $2
		ORDER BY ts DESC, id DESC
		LIMIT 1`
	var out domain.Price
	err := p.pool.QueryRow(ctx, q, exchange, symbol).Scan(&out.Exchange, &out.Symbol, &out.TS, &out.Price)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Price{}, ErrNotFound
	}
	if err != nil {
		return domain.Price{}, fmt.Errorf("query latest: %w", err)
	}
	return out, nil
}

func (p *Postgres) GetHistory(ctx context.Context, exchange, symbol string, from, to time.Time, limit int) ([]domain.Price, error) {
	const q = `
		SELECT ts, price
		FROM prices
		WHERE exchange = $1 AND symbol = $2 AND ts >= $3 AND ts <= $4
		ORDER BY ts ASC, id ASC
		LIMIT $5`
	rows, err := p.pool.Query(ctx, q, exchange, symbol, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	out := make([]domain.Price, 0, 128)
	for rows.Next() {
		var ts time.Time
		var price decimal.Decimal
		if err := rows.Scan(&ts, &price); err != nil {
			return nil, fmt.Errorf("scan history: %w", err)
		}
		out = append(out, domain.Price{
			Exchange: exchange,
			Symbol:   symbol,
			TS:       ts,
			Price:    price,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}
