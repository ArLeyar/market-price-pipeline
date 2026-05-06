package config

import (
	"errors"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	HTTPAddr     string        `envconfig:"HTTP_ADDR" default:":8080"`
	PostgresDSN  string        `envconfig:"POSTGRES_DSN" required:"true"`
	RedisAddr    string        `envconfig:"REDIS_ADDR" required:"true"`
	KafkaBrokers []string      `envconfig:"KAFKA_BROKERS" required:"true"`
	KafkaTopic   string        `envconfig:"KAFKA_TOPIC" default:"prices.ticks"`
	BinanceWSURL string        `envconfig:"BINANCE_WS_URL" default:"wss://stream.binance.com:9443/stream"`
	Symbols      []string      `envconfig:"SYMBOLS" required:"true"`
	LatestTTL    time.Duration `envconfig:"LATEST_TTL" default:"60s"`
	LogLevel     string        `envconfig:"LOG_LEVEL" default:"info"`
}

func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, err
	}

	c.Symbols = cleanList(c.Symbols, strings.ToUpper)
	if len(c.Symbols) == 0 {
		return nil, errors.New("SYMBOLS must contain at least one non-empty value")
	}

	c.KafkaBrokers = cleanList(c.KafkaBrokers, nil)
	if len(c.KafkaBrokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS must contain at least one non-empty value")
	}

	return &c, nil
}

func cleanList(in []string, transform func(string) string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if transform != nil {
			s = transform(s)
		}
		out = append(out, s)
	}
	return out
}
