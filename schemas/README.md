# DuckDB Schemas for Crop-AI

This folder contains idempotent DDL files for initializing DuckDB tables used in the crop price forecasting project. Each file defines a single table or registry, and can be applied in order for reproducible schema setup.

## Usage

To apply all schemas in order, run:

```sh
cat schemas/*.sql | duckdb /db/crop-ai.duckdb
```

Or, from inside the DuckDB CLI:

```sql
.read schemas/00__init_registry.sql
.read schemas/01__market_master.sql
.read schemas/02__openmeteo_bronze_files.sql
.read schemas/03__openmeteo_bronze_rows.sql
.read schemas/04__openmeteo_silver_weather_daily.sql
.read schemas/05__agmarknet_silver_prices_daily.sql
.read schemas/06__gold_features_v1.sql
.read schemas/07__metadata_table_manifest.sql
.read schemas/08__metadata_schema_registry.sql
```

## File List
- 00__init_registry.sql
- 01__market_master.sql
- 02__openmeteo_bronze_files.sql
- 03__openmeteo_bronze_rows.sql
- 04__openmeteo_silver_weather_daily.sql
- 05__agmarknet_silver_prices_daily.sql
- 06__gold_features_v1.sql
- 07__metadata_table_manifest.sql
- 08__metadata_schema_registry.sql

All tables use DuckDB-compatible types. Raw JSON is stored as VARCHAR. See each file for details.
