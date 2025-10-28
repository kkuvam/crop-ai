-- 00__agmarknet_bronze_files.sql
CREATE TABLE IF NOT EXISTS agmarknet_bronze_files (
  file_id           VARCHAR PRIMARY KEY,
  original_filename VARCHAR NOT NULL,
  year              INTEGER,
  month             INTEGER,
  checksum          VARCHAR,
  raw_payload       VARCHAR,  -- raw JSON/text
  row_count         INTEGER,
  partition_path   VARCHAR,  -- suggested parquet path
  parse_status     VARCHAR,  -- PENDING|PARSED|PARSED_WITH_WARNINGS|ERROR
  parse_error      VARCHAR,
  ingest_job_id     VARCHAR,
  ingest_ts         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
