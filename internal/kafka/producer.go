package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/singleflight"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

// Ping cache: opening a TCP connection per /health call is wasteful and
// turns a public unauth endpoint into a Kafka FD-pressure amplifier.
const pingCacheTTL = 5 * time.Second

// Independent timeout for the dial-and-metadata probe. The caller's request
// context (e.g. /health from a flaky client) must not poison the cache.
const pingDialTimeout = 3 * time.Second

type Producer struct {
	writer  *kafka.Writer
	brokers []string
	topic   string

	pingMu  sync.Mutex
	pingAt  time.Time
	pingErr error
	pinged  bool
	pingSF  singleflight.Group
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 50 * time.Millisecond,
			WriteTimeout: 5 * time.Second,
			RequiredAcks: kafka.RequireOne,
			Async:        false,
		},
		brokers: brokers,
		topic:   topic,
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) Publish(ctx context.Context, price domain.Price) error {
	payload, err := json.Marshal(price)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	key := []byte(price.Exchange + ":" + price.Symbol)
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: payload,
		Time:  price.TS,
	}); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

// Ping checks broker connectivity and topic existence, caching the result
// for pingCacheTTL. Concurrent expirations collapse via singleflight.
func (p *Producer) Ping(ctx context.Context) error {
	p.pingMu.Lock()
	if p.pinged && time.Since(p.pingAt) < pingCacheTTL {
		err := p.pingErr
		p.pingMu.Unlock()
		return err
	}
	p.pingMu.Unlock()

	v, err, _ := p.pingSF.Do("ping", func() (any, error) {
		// Independent ctx: a cancelled caller ctx (e.g. dropped /health request)
		// must not be cached as a Kafka outage.
		dialCtx, cancel := context.WithTimeout(context.Background(), pingDialTimeout)
		defer cancel()
		return nil, p.dialPing(dialCtx)
	})
	_ = v
	// Caller-side ctx still controls how long Ping itself blocks.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	p.pingMu.Lock()
	p.pinged = true
	p.pingAt = time.Now()
	p.pingErr = err
	p.pingMu.Unlock()
	return err
}

func (p *Producer) dialPing(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return errors.New("no brokers configured")
	}
	dialer := &kafka.Dialer{Timeout: pingDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Confirm the configured topic exists. Auto-create is disabled in compose,
	// so without this check /health can return 200 while Publish silently fails.
	parts, err := conn.ReadPartitions(p.topic)
	if err != nil {
		return fmt.Errorf("read partitions for %q: %w", p.topic, err)
	}
	if len(parts) == 0 {
		return fmt.Errorf("topic %q has no partitions", p.topic)
	}
	return nil
}
