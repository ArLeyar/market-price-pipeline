CREATE TABLE IF NOT EXISTS prices (
    id       BIGSERIAL PRIMARY KEY,
    exchange TEXT             NOT NULL DEFAULT 'binance',
    symbol   TEXT             NOT NULL,
    ts       TIMESTAMPTZ      NOT NULL,
    price    NUMERIC(38, 18)  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts          ON prices (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_prices_exchange_symbol_ts ON prices (exchange, symbol, ts DESC);
