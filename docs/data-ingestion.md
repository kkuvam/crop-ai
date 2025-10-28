# 🌾 Crop Price Forecasting Project — Simplified Data & ML Pipeline

**Objective:**  
Predict short-term (7-day) crop price changes using Agmarknet price data, supported by eNAM, NCDEX, and Open-Meteo weather data.

This project builds a small but complete data pipeline — from raw data to machine learning — using **DuckDB**, **Parquet**, and **XGBoost**.

---

## 🧭 Project Overview

**Primary data source:**  
- [Agmarknet](https://agmarknet.gov.in/) – daily mandi prices

**Supporting data:**  
- [eNAM](https://enam.gov.in/) – additional spot prices  
- [NCDEX](https://www.ncdex.com/) – futures contracts  
- [Open-Meteo](https://open-meteo.com/) – daily weather data

**Pipeline structure:**
1. **Bronze:** Store raw files exactly as downloaded  
2. **Silver:** Clean and align data by market  
3. **Gold:** Create model-ready features and train XGBoost

---

## 🪙 Layer 1 – Bronze (Raw Data)

Raw files are stored as-is for traceability.  
We use **monthly Parquet shards** partitioned by market (`place_name_norm`) or by location (`lat_tile`, `lon_tile`).

### Tables

#### `openmeteo_bronze_files`
| Column | Type | Description |
|---------|------|-------------|
| `file_id` | VARCHAR | Unique ID for file |
| `original_filename` | VARCHAR | Raw filename |
| `place_name_norm` | VARCHAR | Normalized place name |
| `lat`, `lon` | DOUBLE | Coordinates |
| `year`, `month` | INT | Partition info |
| `checksum` | VARCHAR | File hash |
| `raw_payload` | VARCHAR | Full JSON text |
| `ingest_ts` | TIMESTAMP | Ingestion timestamp |

#### `openmeteo_bronze_rows`
| Column | Type | Description |
|---------|------|-------------|
| `row_id` | VARCHAR | Unique row ID |
| `file_id` | VARCHAR | Linked to bronze file |
| `place_name_norm` | VARCHAR | Location name |
| `lat`, `lon` | DOUBLE | Coordinates |
| `date_utc` | DATE | Date of reading |
| `raw_vars` | JSON | Weather variable dictionary |
| `partition_hint` | VARCHAR | Path hint for storage |

> Each `place_name_norm` × month becomes one Parquet file.

---

## ⚙️ Layer 2 – Silver (Cleaned & Market-Aligned)

Silver data contains **daily weather data per market**.  
We map each place or coordinate to a known `market_id` using a `market_master` table.

### Tables

#### `market_master`
| Column | Type | Description |
|---------|------|-------------|
| `market_id` | VARCHAR | Unique ID |
| `market_name` | VARCHAR | Market name |
| `lat`, `lon` | DOUBLE | Coordinates |

#### `openmeteo_silver_weather_daily`
| Column | Type | Description |
|---------|------|-------------|
| `market_id` | VARCHAR | Mapped market |
| `date_utc` | DATE | Day of observation |
| `temp_mean` | DOUBLE | Mean temperature (°C) |
| `temp_max` | DOUBLE | Max temperature (°C) |
| `precip_mm` | DOUBLE | Rainfall (mm) |
| `rh_mean` | DOUBLE | Relative humidity (%) |
| `wind_mean` | DOUBLE | Wind speed (m/s) |
| `percent_imputed` | DOUBLE | Missing data filled (%) |
| `quality_flag` | SMALLINT | 0=good, 1=imputed, 2=low coverage |
| `upstream_file_ids` | JSON | Source references |

### Simplified Silver Steps
1. Parse and normalize raw weather variables  
2. Map place to `market_id` (via coordinates)  
3. Convert all units to: °C, mm, %, m/s  
4. Fill small missing gaps (≤2 days)  
5. Merge duplicates by **median** values  
6. Flag extreme outliers and set `quality_flag`

---

## 💡 Layer 3 – Gold (Model-Ready Features)

Gold contains daily features used for ML models.  
Each row = **market × date**, representing the features available to predict the next week’s price change.

### Table: `gold_features_v1`
| Column | Type | Description |
|---------|------|-------------|
| `market_id` | VARCHAR | Market ID |
| `date_utc` | DATE | Reference date |
| `price_t` | DOUBLE | Current price |
| `target_pct_change_7d` | DOUBLE | (price_t+7 / price_t) - 1 |
| `temp_lag_1`, `temp_lag_7` | DOUBLE | Temp lags |
| `precip_cum_7` | DOUBLE | 7-day rainfall sum |
| `temp_roll_30`, `temp_std_30` | DOUBLE | 30-day mean/std |
| `days_since_rain` | INT | Drought indicator |
| `day_of_year` | INT | Seasonality |
| `percent_imputed` | DOUBLE | Data quality hint |
| `feature_version` | VARCHAR | Version tag |

---

## 🗄️ DuckDB Schema Overview (2025)

All tables are created via idempotent SQL files in `schemas/`. To initialize the database, run:

```sh
cat schemas/*.sql | duckdb duckdb/cropai.duckdb
```

### Table Definitions

#### `market_master`
| Column         | Type      | Description           |
| -------------- | --------- | --------------------- |
| market_id      | VARCHAR   | Unique market ID      |
| market_name    | VARCHAR   | Market name           |
| state          | VARCHAR   | State                 |
| district       | VARCHAR   | District              |
| lat            | DOUBLE    | Latitude              |
| lon            | DOUBLE    | Longitude             |
| created_at     | TIMESTAMP | Creation timestamp    |

#### `openmeteo_bronze_files`
| Column            | Type      | Description                  |
| ----------------- | --------- | ---------------------------- |
| file_id           | VARCHAR   | Unique file ID               |
| original_filename | VARCHAR   | Raw filename                 |
| country           | VARCHAR   | Country                      |
| state             | VARCHAR   | State                        |
| district          | VARCHAR   | District                     |
| place_name_raw    | VARCHAR   | Raw place name               |
| place_name_norm   | VARCHAR   | Normalized place name        |
| lat               | DOUBLE    | Latitude                     |
| lon               | DOUBLE    | Longitude                    |
| lat_tile          | DOUBLE    | Rounded latitude (partition) |
| lon_tile          | DOUBLE    | Rounded longitude            |
| geo_type          | VARCHAR   | Place/latlon/both            |
| year              | INTEGER   | Year                         |
| month             | INTEGER   | Month                        |
| checksum          | VARCHAR   | File hash                    |
| raw_payload       | VARCHAR   | Raw JSON as text             |
| row_count         | INTEGER   | Number of rows               |
| partition_path    | VARCHAR   | Parquet path                 |
| parse_status      | VARCHAR   | Parse status                 |
| parse_error       | VARCHAR   | Parse error                  |
| ingest_job_id     | VARCHAR   | Ingestion job ID             |
| ingest_ts         | TIMESTAMP | Ingestion timestamp          |
| created_at        | TIMESTAMP | Creation timestamp           |

#### `openmeteo_bronze_rows`
| Column                | Type      | Description                  |
| --------------------- | --------- | ---------------------------- |
| row_id                | VARCHAR   | Unique row ID                |
| file_id               | VARCHAR   | Linked file ID               |
| place_name_raw        | VARCHAR   | Raw place name               |
| place_name_norm       | VARCHAR   | Normalized place name        |
| lat                   | DOUBLE    | Latitude                     |
| lon                   | DOUBLE    | Longitude                    |
| lat_tile              | DOUBLE    | Rounded latitude             |
| lon_tile              | DOUBLE    | Rounded longitude            |
| grid_id               | VARCHAR   | Grid ID                      |
| date_utc              | DATE      | Date (UTC)                   |
| timestamp_utc         | TIMESTAMP | Timestamp (UTC)              |
| raw_vars              | VARCHAR   | JSON string of daily vars    |
| ingest_job_id         | VARCHAR   | Ingestion job ID             |
| row_checksum          | VARCHAR   | Row hash                     |
| parse_warnings        | VARCHAR   | Parse warnings               |
| partition_hint        | VARCHAR   | Storage hint                 |
| original_record_index | INTEGER   | Original record index        |
| created_at            | TIMESTAMP | Creation timestamp           |

#### `openmeteo_silver_weather_daily`
| Column                    | Type      | Description                  |
| ------------------------- | --------- | ---------------------------- |
| weather_id                | VARCHAR   | Unique weather ID            |
| market_id                 | VARCHAR   | Mapped market ID             |
| date_utc                  | DATE      | Date (UTC)                   |
| lat                       | DOUBLE    | Latitude                     |
| lon                       | DOUBLE    | Longitude                    |
| temperature_2m_mean       | DOUBLE    | Mean temperature             |
| temperature_2m_max        | DOUBLE    | Max temperature              |
| temperature_2m_min        | DOUBLE    | Min temperature              |
| cloud_cover_mean          | DOUBLE    | Mean cloud cover             |
| relative_humidity_2m_mean | DOUBLE    | Mean relative humidity       |
| relative_humidity_2m_max  | DOUBLE    | Max relative humidity        |
| relative_humidity_2m_min  | DOUBLE    | Min relative humidity        |
| wind_speed_10m_mean       | DOUBLE    | Mean wind speed              |
| wind_speed_10m_max        | DOUBLE    | Max wind speed               |
| wind_speed_10m_min        | DOUBLE    | Min wind speed               |
| wet_bulb_temperature_2m_mean | DOUBLE | Mean wet bulb temp           |
| wet_bulb_temperature_2m_max  | DOUBLE | Max wet bulb temp            |
| wet_bulb_temperature_2m_min  | DOUBLE | Min wet bulb temp            |
| wind_direction_10m_dominant  | INTEGER| Dominant wind direction      |
| precip_mm                 | DOUBLE    | Precipitation (mm)           |
| precip_alt_mm             | DOUBLE    | Alternate precipitation      |
| data_source               | VARCHAR   | Data source                  |
| upstream_file_ids         | VARCHAR   | JSON list of file IDs        |
| percent_imputed           | DOUBLE    | % imputed data               |
| quality_flag              | SMALLINT  | Data quality flag            |
| imputed_by                | VARCHAR   | Imputation source            |
| cleaning_version          | VARCHAR   | Cleaning version             |
| created_at                | TIMESTAMP | Creation timestamp           |

#### `agmarknet_silver_prices_daily`
| Column        | Type      | Description                  |
| ------------- | --------- | ---------------------------- |
| price_id      | VARCHAR   | Unique price ID              |
| market_id     | VARCHAR   | Market ID                    |
| crop_id       | VARCHAR   | Crop ID                      |
| date_utc      | DATE      | Date (UTC)                   |
| price_type    | VARCHAR   | Price type                   |
| price_value   | DOUBLE    | Price value                  |
| unit          | VARCHAR   | Unit                         |
| source        | VARCHAR   | Data source                  |
| source_ts     | TIMESTAMP | Source timestamp             |
| created_at    | TIMESTAMP | Creation timestamp           |

#### `gold_features_v1`
| Column                  | Type      | Description                  |
| ----------------------- | --------- | ---------------------------- |
| feature_id              | VARCHAR   | Unique feature ID            |
| market_id               | VARCHAR   | Market ID                    |
| crop_id                 | VARCHAR   | Crop ID (optional)           |
| date_utc                | DATE      | Reference date               |
| feature_generation_ts   | TIMESTAMP | Feature generation time      |
| feature_version         | VARCHAR   | Feature version              |
| temp_lag_1              | DOUBLE    | Temp 1 day ago               |
| temp_lag_7              | DOUBLE    | Temp 7 days ago              |
| precip_cum_7            | DOUBLE    | 7-day rainfall sum           |
| temp_roll_30            | DOUBLE    | 30-day rolling mean temp     |
| temp_std_30             | DOUBLE    | 30-day rolling temp std      |
| days_since_rain         | INTEGER   | Days since last rain         |
| day_of_year             | INTEGER   | Calendar day                 |
| doy_sin                 | DOUBLE    | Sine of day of year          |
| doy_cos                 | DOUBLE    | Cosine of day of year        |
| percent_imputed         | DOUBLE    | % imputed data               |
| price_t                 | DOUBLE    | Current price                |
| target_pct_change_7d    | DOUBLE    | 7-day price change           |
| upstream_silver_version | VARCHAR   | Upstream silver version      |
| lineage_json            | VARCHAR   | Upstream lineage JSON        |

#### `metadata_table_manifest`
| Column        | Type      | Description                  |
| ------------- | --------- | ---------------------------- |
| table_name    | VARCHAR   | Table name                   |
| purpose       | VARCHAR   | Table purpose                |
| primary_keys  | VARCHAR   | Primary keys                 |
| partitioning  | VARCHAR   | Partitioning info            |
| last_verified | TIMESTAMP | Last verified timestamp      |

#### `metadata_schema_registry`
| Column         | Type      | Description                  |
| -------------- | --------- | ---------------------------- |
| table_name     | VARCHAR   | Table name                   |
| column_name    | VARCHAR   | Column name                  |
| data_type      | VARCHAR   | Data type                    |
| is_nullable    | BOOLEAN   | Is nullable                  |
| column_default | VARCHAR   | Default value                |
| description    | VARCHAR   | Column description           |
| schema_version | VARCHAR   | Schema version               |
| created_at     | TIMESTAMP | Creation timestamp           |

---

## 📈 Training & Evaluation

### Label definition

- Use **Agmarknet** as the main price source.  
- Fill gaps using **eNAM**, then **NCDEX** if available.

### Model setup
- Algorithm: **XGBoost Regressor**  
- Input: features listed above  
- Output: predicted 7-day % change in price  
- Metric: **MAE** (Mean Absolute Error) and **direction accuracy**

### Split strategy
| Dataset | Period | Purpose |
|----------|---------|---------|
| Train | All data until last 90 days | Model training |
| Validation | 90 → 30 days ago | Tune hyperparams |
| Test | Last 30 days | Evaluate prediction power |

> Don’t shuffle — use time-based splits.

---

## 🧰 Tooling

- **DuckDB** – database for Parquet and SQL transformations  
- **Parquet** – compact data storage (month-wise)  
- **Python + XGBoost + Pandas** – modeling and visualization  
- **Jupyter Notebook** – for analysis and reports

---

## Pipeline
- ingest_bronze — fetch / copy JSONs, register openmeteo_bronze_files metadata, write monthly Parquet shards.

- parse_bronze — parse JSON rows into openmeteo_bronze_rows and compute partition hints + checksums.

- silver_clean — map place → market_id, normalize units, dedupe & median-merge, impute small gaps, write openmeteo_silver_weather_daily.

- gold_build — compute lag/rolling features and join with price tables to produce gold_features_v1 snapshot.

- train_model — train XGBoost on a given snapshot and output model_{version}.pkl + metrics.

- score — run the model against the live feature view and write predictions.

- monitor (optional small script) — compute simple run metrics and append to run_log.csv.

## ✅ Quality Checks

| Layer | Check | Pass Condition |
|--------|--------|----------------|
| Bronze | File ingested | Row count > 0 |
| Silver | Data completeness | >50% days per month available |
| Gold | Label coverage | >30% rows have target |
| Model | Performance | Direction accuracy >55% |

---

## 🗓️ Suggested 5-Week Timeline

| Week | Focus | Output |
|-------|--------|---------|
| 1 | Bronze ingestion | JSON → Parquet |
| 2 | Silver cleaning | Daily weather per market |
| 3 | Gold features | Features + Labels |
| 4 | Model training | XGBoost baseline |
| 5 | Report & visualization | Insights, charts, accuracy |

---

## 📊 Simple Monitoring

Maintain a CSV log: `run_log.csv`

| Date | Rows_Bronze | Rows_Silver | Rows_Gold | Imputed_% | MAE | Notes |
|------|--------------|--------------|------------|-----------|-----|-------|

Plot:
- Validation MAE vs time  
- % Imputed vs time

---

## 🧩 Summary

This pipeline follows a **Bronze → Silver → Gold** pattern simplified for student use:

- **Bronze:** store raw files safely  
- **Silver:** clean, align, and prepare daily market-level data  
- **Gold:** compute simple rolling weather features and labels  
- **Model:** train XGBoost to predict 7-day price movements  

You’ll learn:
- Data ingestion & cleaning  
- Feature engineering  
- Time-series modeling  
- Result evaluation & iteration  


