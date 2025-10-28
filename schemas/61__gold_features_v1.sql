-- 06__gold_features_v1.sql
CREATE TABLE IF NOT EXISTS gold_features_v1 (
  feature_id            VARCHAR PRIMARY KEY,
  market_id             VARCHAR NOT NULL,
  crop_id               VARCHAR,    -- optional
  date_utc              DATE NOT NULL, -- reference date t
  feature_generation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  feature_version       VARCHAR NOT NULL,
  -- features (small core set for class project)
  temp_lag_1            DOUBLE,
  temp_lag_7            DOUBLE,
  precip_cum_7          DOUBLE,
  temp_roll_30          DOUBLE,
  temp_std_30           DOUBLE,
  days_since_rain       INTEGER,
  day_of_year           INTEGER,
  doy_sin               DOUBLE,
  doy_cos               DOUBLE,
  percent_imputed       DOUBLE,
  price_t               DOUBLE,   -- current spot price (for label calculations)
  target_pct_change_7d  DOUBLE,   -- label (nullable)
  upstream_silver_version VARCHAR,
  lineage_json          VARCHAR    -- small JSON describing upstream versions
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_market_date ON gold_features_v1(market_id, date_utc);
