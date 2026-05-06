package binance

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseBookTickerEnvelope(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantErr   bool
		wantSym   string
		wantPrice string // exact decimal repr of mid price
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
			// Exercises NUMERIC(38,18) precision: input is at the schema's
			// fractional limit (18 digits). The parser must not truncate.
			name:      "high-precision sub-cent",
			input:     `{"u":1,"s":"DUSTBTC","b":"0.000000000000000001","B":"1","a":"0.000000000000000003","A":"1"}`,
			wantSym:   "DUSTBTC",
			wantPrice: "0.000000000000000002",
		},
		{
			// Mid of bid+ask with mismatched scales must round-trip exactly.
			name:      "mixed scales",
			input:     `{"u":1,"s":"BTCUSDT","b":"60000.1","B":"1","a":"60000.3","A":"1"}`,
			wantSym:   "BTCUSDT",
			wantPrice: "60000.2",
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
		{
			name:    "zero price rejected (mid would be 0)",
			input:   `{"u":1,"s":"BTCUSDT","b":"0","B":"1","a":"0","A":"1"}`,
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
			want := decimal.RequireFromString(tc.wantPrice)
			if !p.Price.Equal(want) {
				t.Errorf("price: got %s, want %s", p.Price.String(), want.String())
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
