package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

// Ping cache: opening a TCP connection per /health call is wasteful and
// turns a public unauth endpoint into a Kafka FD-pressure amplifier.
const pingCacheTTL = 5 * time.Second

type Producer struct {
	writer  *kafka.Writer
	brokers []string
	topic   string

	pingMu  sync.Mutex
	pingAt  time.Time
	pingErr error
	pinged  bool
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

// Ping checks broker connectivity, caching the result for pingCacheTTL.
func (p *Producer) Ping(ctx context.Context) error {
	p.pingMu.Lock()
	if p.pinged && time.Since(p.pingAt) < pingCacheTTL {
		err := p.pingErr
		p.pingMu.Unlock()
		return err
	}
	p.pingMu.Unlock()

	err := p.dialPing(ctx)

	p.pingMu.Lock()
	p.pinged = true
	p.pingAt = time.Now()
	p.pingErr = err
	p.pingMu.Unlock()
	return err
}

func (p *Producer) dialPing(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	dialer := &kafka.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Brokers(); err != nil {
		return fmt.Errorf("brokers: %w", err)
	}
	return nil
}
