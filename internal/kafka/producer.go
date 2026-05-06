package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

type Producer struct {
	writer  *kafka.Writer
	brokers []string
	topic   string
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 50 * time.Millisecond,
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

// Ping checks broker connectivity by dialing the first broker and listing topics.
func (p *Producer) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	dialer := &kafka.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	if _, err := conn.Brokers(); err != nil {
		return fmt.Errorf("brokers: %w", err)
	}
	return nil
}
