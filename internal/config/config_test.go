package config

import (
	"testing"
)

func TestCleanList(t *testing.T) {
	cases := []struct {
		name      string
		in        []string
		transform func(string) string
		want      []string
	}{
		{"trim spaces", []string{" a", "b ", "c"}, nil, []string{"a", "b", "c"}},
		{"drop empty", []string{"", "a", "  ", "b"}, nil, []string{"a", "b"}},
		{"all empty", []string{"", " ", "\t"}, nil, []string{}},
		{"transform applied after trim", []string{" btcusdt", "ETHUSDT "}, func(s string) string {
			out := []byte(s)
			for i, c := range out {
				if c >= 'a' && c <= 'z' {
					out[i] = c - 32
				}
			}
			return string(out)
		}, []string{"BTCUSDT", "ETHUSDT"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanList(tc.in, tc.transform)
			if len(got) != len(tc.want) {
				t.Fatalf("len: got %d %v, want %d %v", len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("[%d]: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestLoad_RejectsEmptySymbols(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "x")
	t.Setenv("REDIS_ADDR", "x")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("SYMBOLS", " , , ")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for all-empty SYMBOLS, got nil")
	}
}

func TestLoad_RejectsEmptyBrokers(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "x")
	t.Setenv("REDIS_ADDR", "x")
	t.Setenv("KAFKA_BROKERS", " , ")
	t.Setenv("SYMBOLS", "BTCUSDT")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for all-empty KAFKA_BROKERS, got nil")
	}
}

func TestLoad_NormalizesAndDedupes(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "x")
	t.Setenv("REDIS_ADDR", "x")
	t.Setenv("KAFKA_BROKERS", "kafka:9092 , , kafka2:9092")
	t.Setenv("SYMBOLS", " btcusdt , , ETHUSDT ")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Symbols) != 2 || c.Symbols[0] != "BTCUSDT" || c.Symbols[1] != "ETHUSDT" {
		t.Errorf("Symbols: %v", c.Symbols)
	}
	if len(c.KafkaBrokers) != 2 || c.KafkaBrokers[0] != "kafka:9092" || c.KafkaBrokers[1] != "kafka2:9092" {
		t.Errorf("KafkaBrokers: %v", c.KafkaBrokers)
	}
}
