package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/arleyar/market-price-pipeline/internal/domain"
	"github.com/arleyar/market-price-pipeline/internal/storage"
)

const (
	defaultExchange = "binance"
	defaultLimit    = 1000
	maxLimit        = 10000
)

type Handlers struct {
	deps Deps
}

type priceDTO struct {
	Exchange string    `json:"exchange"`
	Symbol   string    `json:"symbol"`
	TS       time.Time `json:"ts"`
	Price    string    `json:"price"`
}

func toDTO(p domain.Price) priceDTO {
	return priceDTO{
		Exchange: p.Exchange,
		Symbol:   p.Symbol,
		TS:       p.TS,
		Price:    p.Price.String(),
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{
		"db":    "up",
		"redis": "up",
		"kafka": "up",
	}
	overallOK := true
	if err := h.deps.DBPing.Ping(ctx); err != nil {
		checks["db"] = "down"
		overallOK = false
	}
	if err := h.deps.RDPing.Ping(ctx); err != nil {
		checks["redis"] = "down"
		overallOK = false
	}
	if err := h.deps.KFPing.Ping(ctx); err != nil {
		checks["kafka"] = "down"
		overallOK = false
	}
	status := "ok"
	code := http.StatusOK
	if !overallOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status": status,
		"db":     checks["db"],
		"redis":  checks["redis"],
		"kafka":  checks["kafka"],
	})
}

func (h *Handlers) Latest(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return
	}
	exchange := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("exchange")))
	if exchange == "" {
		exchange = defaultExchange
	}

	if h.deps.Cache != nil {
		if p, ok, err := h.deps.Cache.GetLatest(r.Context(), exchange, symbol); err == nil && ok {
			writeJSON(w, http.StatusOK, toDTO(p))
			return
		}
	}

	p, err := h.deps.DB.GetLatest(r.Context(), exchange, symbol)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "no prices for symbol")
			return
		}
		h.deps.Log.Error("latest db failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toDTO(p))
}

func (h *Handlers) History(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	symbol := strings.ToUpper(strings.TrimSpace(q.Get("symbol")))
	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return
	}
	exchange := strings.ToLower(strings.TrimSpace(q.Get("exchange")))
	if exchange == "" {
		exchange = defaultExchange
	}
	from, err := time.Parse(time.RFC3339, q.Get("from"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "from must be RFC3339")
		return
	}
	to, err := time.Parse(time.RFC3339, q.Get("to"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "to must be RFC3339")
		return
	}
	if !from.Before(to) {
		writeError(w, http.StatusBadRequest, "from must be < to")
		return
	}
	limit := defaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > maxLimit {
			n = maxLimit
		}
		limit = n
	}

	rows, err := h.deps.History.GetHistory(r.Context(), exchange, symbol, from, to, limit)
	if err != nil {
		h.deps.Log.Error("history db failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	out := make([]priceDTO, 0, len(rows))
	for _, p := range rows {
		out = append(out, toDTO(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": out,
		"count": len(out),
	})
}
