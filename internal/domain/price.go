package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type Price struct {
	Exchange string          `json:"exchange"`
	Symbol   string          `json:"symbol"`
	TS       time.Time       `json:"ts"`
	Price    decimal.Decimal `json:"price"`
}
