-- 03__openmeteo_bronze_rows.sql
CREATE TABLE IF NOT EXISTS openmeteo_bronze_rows (
  row_id              VARCHAR PRIMARY KEY,
  file_id             VARCHAR NOT NULL,
  place_name_raw      VARCHAR,
  place_name_norm     VARCHAR,
  lat                 DOUBLE,
  lon                 DOUBLE,
  lat_tile            DOUBLE,
  lon_tile            DOUBLE,
  grid_id             VARCHAR,
  date_utc            DATE NOT NULL,
  timestamp_utc       TIMESTAMP,
  raw_vars            VARCHAR NOT NULL,  -- JSON string of DAILY_VARS
  ingest_job_id       VARCHAR,
  row_checksum        VARCHAR,
  parse_warnings      VARCHAR,
  partition_hint      VARCHAR,
  original_record_index INTEGER,
  created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (file_id) REFERENCES openmeteo_bronze_files(file_id)
);
-- Index-like hint for faster queries (DuckDB will benefit from appropriate physical partitioning)
CREATE VIEW IF NOT EXISTS openmeteo_bronze_rows_view AS
SELECT row_id, file_id, place_name_norm, lat, lon, date_utc FROM openmeteo_bronze_rows;
