-- 02__openmeteo_bronze_files.sql
CREATE TABLE IF NOT EXISTS openmeteo_bronze_files (
  file_id          VARCHAR PRIMARY KEY,
  original_filename VARCHAR NOT NULL,
  country          VARCHAR,
  state            VARCHAR,
  district         VARCHAR,
  place_name_raw   VARCHAR,
  place_name_norm  VARCHAR,
  lat              DOUBLE,
  lon              DOUBLE,
  lat_tile         DOUBLE,   -- e.g., rounded lat for partitioning
  lon_tile         DOUBLE,
  geo_type         VARCHAR,  -- 'place_name'|'latlon'|'both'
  year             INTEGER,
  month            INTEGER,
  checksum         VARCHAR,
  raw_payload      VARCHAR,  -- raw JSON as text
  row_count        INTEGER,
  partition_path   VARCHAR,  -- suggested parquet path
  parse_status     VARCHAR,  -- PENDING|PARSED|PARSED_WITH_WARNINGS|ERROR
  parse_error      VARCHAR,
  ingest_job_id    VARCHAR,
  ingest_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
