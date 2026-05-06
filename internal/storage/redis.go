package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

type Redis struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedis(addr string, ttl time.Duration) *Redis {
	return &Redis{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		ttl:    ttl,
	}
}

func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func latestKey(exchange, symbol string) string {
	return fmt.Sprintf("price:%s:%s", exchange, symbol)
}

func (r *Redis) SetLatest(ctx context.Context, p domain.Price) error {
	payload, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := r.client.Set(ctx, latestKey(p.Exchange, p.Symbol), payload, r.ttl).Err(); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	return nil
}

func (r *Redis) GetLatest(ctx context.Context, exchange, symbol string) (domain.Price, bool, error) {
	v, err := r.client.Get(ctx, latestKey(exchange, symbol)).Bytes()
	if errors.Is(err, redis.Nil) {
		return domain.Price{}, false, nil
	}
	if err != nil {
		return domain.Price{}, false, fmt.Errorf("get: %w", err)
	}
	var out domain.Price
	if err := json.Unmarshal(v, &out); err != nil {
		return domain.Price{}, false, fmt.Errorf("unmarshal: %w", err)
	}
	return out, true, nil
}
