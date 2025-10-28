-- 33__enam_bronze_rows.sql
CREATE TABLE IF NOT EXISTS enam_bronze_rows (
  row_id              VARCHAR PRIMARY KEY,
  file_id             VARCHAR NOT NULL,
  apmc                VARCHAR,
  state               VARCHAR,
  commodity           VARCHAR,
  reported_date       DATE NOT NULL,
  timestamp_utc       TIMESTAMP,
  raw_vars            VARCHAR,  -- original JSON/text
  ingest_job_id       VARCHAR,
  row_checksum        VARCHAR,
  parse_warnings      VARCHAR,
  partition_hint      VARCHAR,
  original_record_index INTEGER,
  loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (file_id) REFERENCES enam_bronze_files(file_id)
);
