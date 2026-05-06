package config

import (
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
	for i, s := range c.Symbols {
		c.Symbols[i] = strings.ToUpper(strings.TrimSpace(s))
	}
	return &c, nil
}
