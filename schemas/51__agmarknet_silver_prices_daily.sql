-- 05__agmarknet_silver_prices_daily.sql
CREATE TABLE IF NOT EXISTS agmarknet_silver_prices_daily (
  price_id       VARCHAR PRIMARY KEY,
  market_id      VARCHAR NOT NULL,
  crop_id        VARCHAR NOT NULL,
  date_utc       DATE NOT NULL,
  price_type     VARCHAR,    -- modal/arrival/min/max etc
  price_value    DOUBLE,     -- canonical unit, e.g., INR per quintal
  unit           VARCHAR,    -- e.g., "quintal" or "kg"
  source         VARCHAR,    -- 'agmarknet'|'enam'|'ncdex'
  source_ts      TIMESTAMP,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(market_id, crop_id, date_utc, price_type)
);
