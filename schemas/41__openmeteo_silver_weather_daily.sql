-- 04__openmeteo_silver_weather_daily.sql
CREATE TABLE IF NOT EXISTS openmeteo_silver_weather_daily (
  weather_id           VARCHAR PRIMARY KEY,
  market_id            VARCHAR NOT NULL,
  date_utc             DATE NOT NULL,
  lat                  DOUBLE,
  lon                  DOUBLE,
  temperature_2m_mean  DOUBLE,
  temperature_2m_max   DOUBLE,
  temperature_2m_min   DOUBLE,
  cloud_cover_mean     DOUBLE,
  relative_humidity_2m_mean DOUBLE,
  relative_humidity_2m_max  DOUBLE,
  relative_humidity_2m_min  DOUBLE,
  wind_speed_10m_mean  DOUBLE,
  wind_speed_10m_max   DOUBLE,
  wind_speed_10m_min   DOUBLE,
  wet_bulb_temperature_2m_mean DOUBLE,
  wet_bulb_temperature_2m_max  DOUBLE,
  wet_bulb_temperature_2m_min  DOUBLE,
  wind_direction_10m_dominant INTEGER,
  precip_mm            DOUBLE,   -- canonical precipitation column
  precip_alt_mm        DOUBLE,   -- alternative name if both present
  data_source          VARCHAR,
  upstream_file_ids    VARCHAR,   -- JSON list of file_id strings
  percent_imputed      DOUBLE,
  quality_flag         SMALLINT,
  imputed_by           VARCHAR,
  cleaning_version     VARCHAR,
  created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(market_id, date_utc)
);
