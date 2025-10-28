-- 05__ncdex_bronze_rows.sql
CREATE TABLE IF NOT EXISTS ncdex_bronze_rows (
  row_id              VARCHAR PRIMARY KEY,
  file_id             VARCHAR NOT NULL,
  crop_name           VARCHAR,
  contract_type       VARCHAR,
  price_value         DOUBLE,
  unit                VARCHAR,
  date_utc            DATE,
  raw_vars            VARCHAR,  -- JSON string or raw record
  ingest_job_id       VARCHAR,
  row_checksum        VARCHAR,
  parse_warnings      VARCHAR,
  partition_hint      VARCHAR,
  original_record_index INTEGER,
  created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (file_id) REFERENCES ncdex_bronze_files(file_id)
);
