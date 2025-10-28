-- 01__market_master.sql
CREATE TABLE IF NOT EXISTS market_master (
  market_id      VARCHAR PRIMARY KEY,
  market_name    VARCHAR NOT NULL,
  state          VARCHAR,
  district       VARCHAR,
  lat            DOUBLE,
  lon            DOUBLE,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
