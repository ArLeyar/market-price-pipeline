package binance

import (
	"strings"
	"testing"
)

func TestParseBookTickerEnvelope(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantErr   bool
		wantSym   string
		wantPrice string // string repr of mid price
	}{
		{
			name:      "raw bookTicker",
			input:     `{"u":12345,"s":"BTCUSDT","b":"60000.00","B":"1","a":"60002.00","A":"1"}`,
			wantSym:   "BTCUSDT",
			wantPrice: "60001",
		},
		{
			name:      "combined-stream envelope",
			input:     `{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"100","B":"1","a":"102","A":"1"}}`,
			wantSym:   "BTCUSDT",
			wantPrice: "101",
		},
		{
			name:    "empty",
			input:   ``,
			wantErr: true,
		},
		{
			name:    "malformed json",
			input:   `{"s":"BTCUSDT", "b":`,
			wantErr: true,
		},
		{
			name:    "missing fields",
			input:   `{"u":1,"s":"BTCUSDT"}`,
			wantErr: true,
		},
		{
			name:    "non-numeric price",
			input:   `{"u":1,"s":"BTCUSDT","b":"abc","B":"1","a":"1","A":"1"}`,
			wantErr: true,
		},
		{
			name:    "negative price",
			input:   `{"u":1,"s":"BTCUSDT","b":"-1","B":"1","a":"1","A":"1"}`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseBookTickerEnvelope([]byte(tc.input))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; price=%+v", p)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.Symbol != tc.wantSym {
				t.Errorf("symbol: got %s, want %s", p.Symbol, tc.wantSym)
			}
			if got := p.Price.String(); !strings.HasPrefix(got, tc.wantPrice) {
				t.Errorf("price: got %s, want prefix %s", got, tc.wantPrice)
			}
		})
	}
}

func TestBuildStreamURL(t *testing.T) {
	got, err := buildStreamURL("wss://example/stream", []string{"BTCUSDT", "ETHUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	want := "wss://example/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker"
	if got != want {
		t.Errorf("got %s\nwant %s", got, want)
	}
	if _, err := buildStreamURL("wss://x", nil); err == nil {
		t.Error("expected error on empty symbols")
	}
}
