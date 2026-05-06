package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

const (
	exchangeName = "binance"
	readDeadline = 60 * time.Second
)

// TickHandler is invoked for every parsed price update.
type TickHandler func(ctx context.Context, p domain.Price) error

type Stream struct {
	url     string
	symbols []string
	log     *slog.Logger
}

func NewStream(url string, symbols []string, log *slog.Logger) *Stream {
	return &Stream{url: url, symbols: symbols, log: log}
}

// Run blocks until ctx is cancelled. On disconnect it reconnects with
// exponential backoff (1s..30s, with jitter). Backoff resets to 1s once a
// session receives at least one message — without that, transient flaps
// would permanently pin reconnect delay at maxBackoff.
func (s *Stream) Run(ctx context.Context, handle TickHandler) error {
	const (
		initialBackoff = time.Second
		maxBackoff     = 30 * time.Second
	)
	backoff := initialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.runOnce(ctx, handle, func() { backoff = initialBackoff })
		if err == nil || errors.Is(err, context.Canceled) {
			return err
		}
		s.log.Warn("ws session ended, reconnecting", "err", err, "backoff", backoff)

		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (s *Stream) runOnce(ctx context.Context, handle TickHandler, onConnected func()) error {
	streamURL, err := buildStreamURL(s.url, s.symbols)
	if err != nil {
		return fmt.Errorf("build url: %w", err)
	}
	s.log.Info("connecting to binance", "url", streamURL)

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()

	conn, _, err := websocket.Dial(dialCtx, streamURL, &websocket.DialOptions{})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "bye") }()

	conn.SetReadLimit(1 << 20) // 1MiB

	firstMessage := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		readCtx, cancel := context.WithTimeout(ctx, readDeadline)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if firstMessage {
			firstMessage = false
			if onConnected != nil {
				onConnected()
			}
		}
		price, err := ParseBookTickerEnvelope(raw)
		if err != nil {
			s.log.Warn("parse ws message failed", "err", err, "raw", truncate(raw, 256))
			continue
		}
		if err := handle(ctx, price); err != nil {
			s.log.Error("handle tick failed", "err", err, "symbol", price.Symbol)
		}
	}
}

func truncate(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "...[truncated]"
}

// buildStreamURL constructs the combined-stream URL for given symbols.
// Example: wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker
func buildStreamURL(base string, symbols []string) (string, error) {
	if len(symbols) == 0 {
		return "", errors.New("no symbols configured")
	}
	parts := make([]string, 0, len(symbols))
	for _, s := range symbols {
		parts = append(parts, strings.ToLower(s)+"@bookTicker")
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + "streams=" + strings.Join(parts, "/"), nil
}

// bookTickerEvent is the payload Binance pushes for each book ticker update.
// Reference: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams
type bookTickerEvent struct {
	UpdateID int64  `json:"u"`
	Symbol   string `json:"s"`
	BidPrice string `json:"b"`
	BidQty   string `json:"B"`
	AskPrice string `json:"a"`
	AskQty   string `json:"A"`
}

// streamEnvelope wraps payloads when using combined streams (?streams=...).
type streamEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// ParseBookTickerEnvelope handles both raw bookTicker payload and the
// combined-stream envelope format. Returns a domain.Price with the mid price.
func ParseBookTickerEnvelope(raw []byte) (domain.Price, error) {
	if len(raw) == 0 {
		return domain.Price{}, errors.New("empty message")
	}
	payload := raw
	var env streamEnvelope
	if err := json.Unmarshal(raw, &env); err == nil && env.Stream != "" && len(env.Data) > 0 {
		payload = env.Data
	}
	var ev bookTickerEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		return domain.Price{}, fmt.Errorf("unmarshal: %w", err)
	}
	if ev.Symbol == "" || ev.BidPrice == "" || ev.AskPrice == "" {
		return domain.Price{}, errors.New("missing required fields")
	}
	bid, err := decimal.NewFromString(ev.BidPrice)
	if err != nil {
		return domain.Price{}, fmt.Errorf("parse bid: %w", err)
	}
	ask, err := decimal.NewFromString(ev.AskPrice)
	if err != nil {
		return domain.Price{}, fmt.Errorf("parse ask: %w", err)
	}
	if bid.Sign() <= 0 || ask.Sign() <= 0 {
		return domain.Price{}, errors.New("non-positive price")
	}
	// DivRound with 18 fractional digits matches the NUMERIC(38,18) schema.
	// Plain .Div() uses shopspring's default precision (16), which truncates
	// sub-cent prices like 0.000000000000000002 to zero.
	mid := bid.Add(ask).DivRound(decimal.NewFromInt(2), 18)
	return domain.Price{
		Exchange: exchangeName,
		Symbol:   strings.ToUpper(ev.Symbol),
		TS:       time.Now().UTC(),
		Price:    mid,
	}, nil
}
