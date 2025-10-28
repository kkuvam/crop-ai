-- 32__agmarknet_bronze_rows.sql
CREATE TABLE IF NOT EXISTS agmarknet_bronze_rows (
  row_id              VARCHAR PRIMARY KEY,
  file_id             VARCHAR NOT NULL,
  market_name         VARCHAR,
  state_name          VARCHAR,
  district_name       VARCHAR,
  commodity_name      VARCHAR,
  reported_date       DATE NOT NULL,
  timestamp_utc       TIMESTAMP,
  raw_vars            VARCHAR,  -- original JSON/text
  ingest_job_id       VARCHAR,
  row_checksum        VARCHAR,
  parse_warnings      VARCHAR,
  partition_hint      VARCHAR,
  original_record_index INTEGER,
  created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (file_id) REFERENCES agmarknet_bronze_files(file_id)
);
