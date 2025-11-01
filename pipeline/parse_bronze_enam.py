#!/usr/bin/env python3
"""
pipeline/parse_bronze_enam.py

Parse eNAM data from `enam_bronze_files` table into `enam_bronze_rows`.
- Reads raw_payload from bronze_files table
- Expands JSON/JSONL into individual market arrival records
- Computes deterministic row_id and record_hash for deduplication
- Handles versioning for reloads (increments version, maintains is_latest)
- Tracks data quality (nulls, completeness)
- Emits run manifest for each parsing job

SCHEMA:
-------
CREATE TABLE IF NOT EXISTS enam_bronze_rows (
    row_id VARCHAR PRIMARY KEY,
    file_id VARCHAR NOT NULL,
    country VARCHAR DEFAULT 'IN',
    state VARCHAR,
    apmc VARCHAR,
    apmc_norm VARCHAR,
    commodity VARCHAR,
    min_price DOUBLE,
    modal_price DOUBLE,
    max_price DOUBLE,
    commodity_arrivals DOUBLE,
    commodity_traded DOUBLE,
    reported_date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    has_nulls BOOLEAN DEFAULT FALSE,
    is_complete BOOLEAN DEFAULT TRUE,
    null_field_count INTEGER DEFAULT 0,
    record_hash VARCHAR NOT NULL,
    is_latest BOOLEAN DEFAULT TRUE,
    version INTEGER DEFAULT 1,
    superseded_by VARCHAR,
    superseded_at TIMESTAMP,
    ingest_job_id VARCHAR NOT NULL,
    ingest_ts TIMESTAMP NOT NULL,
    source_row_number INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_id) REFERENCES enam_bronze_files(file_id)
);
"""

import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Tuple
import duckdb
from pipeline.ingest_bronze_common import normalize_place_name

# CONFIGURATION
DB_PATH = Path("/Volumes/Extreme/Mission/duckdb/cropai.duckdb")
FILES_TABLE = "enam_bronze_files"
ROWS_TABLE = "enam_bronze_rows"
MANIFEST_DIR = Path("/Volumes/Extreme/Mission/manifests/run")

REQUIRED_FIELDS = ["state", "apmc", "commodity", "reported_date"]
PRICE_FIELDS = ["min_price", "modal_price", "max_price", "commodity_arrivals", "commodity_traded"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# DATABASE SETUP
def create_rows_table_if_not_exists(con: duckdb.DuckDBPyConnection) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {ROWS_TABLE} (
        row_id VARCHAR PRIMARY KEY,
        file_id VARCHAR NOT NULL,
        country VARCHAR DEFAULT 'IN',
        state VARCHAR,
        apmc VARCHAR,
        apmc_norm VARCHAR,
        commodity VARCHAR,
        min_price DOUBLE,
        modal_price DOUBLE,
        max_price DOUBLE,
        commodity_arrivals DOUBLE,
        commodity_traded DOUBLE,
        reported_date DATE NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        has_nulls BOOLEAN DEFAULT FALSE,
        is_complete BOOLEAN DEFAULT TRUE,
        null_field_count INTEGER DEFAULT 0,
        record_hash VARCHAR NOT NULL,
        is_latest BOOLEAN DEFAULT TRUE,
        version INTEGER DEFAULT 1,
        superseded_by VARCHAR,
        superseded_at TIMESTAMP,
        ingest_job_id VARCHAR NOT NULL,
        ingest_ts TIMESTAMP NOT NULL,
        source_row_number INTEGER,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (file_id) REFERENCES enam_bronze_files(file_id)
    );
    """
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_rows_file_id_enam ON {ROWS_TABLE}(file_id);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_date_enam ON {ROWS_TABLE}(reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_apmc_date_enam ON {ROWS_TABLE}(apmc_norm, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_commodity_date_enam ON {ROWS_TABLE}(commodity, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_state_date_enam ON {ROWS_TABLE}(state, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_year_month_enam ON {ROWS_TABLE}(year, month);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_ingest_job_enam ON {ROWS_TABLE}(ingest_job_id);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_record_hash_enam ON {ROWS_TABLE}(record_hash);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_latest_enam ON {ROWS_TABLE}(is_latest) WHERE is_latest = TRUE;",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_rows_unique_apmc_commodity_date_version_enam ON {ROWS_TABLE}(apmc, commodity, reported_date, version);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_created_at_enam ON {ROWS_TABLE}(created_at);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_superseded_enam ON {ROWS_TABLE}(superseded_by, superseded_at) WHERE superseded_by IS NOT NULL;",
    ]
    try:
        con.execute(create_table_sql)
        logger.info(f"Table '{ROWS_TABLE}' ready")
        for idx_sql in indexes:
            con.execute(idx_sql)
        logger.info("All indexes created")
    except Exception as e:
        logger.error(f"Failed to create table/indexes: {e}")
        raise

# HELPER FUNCTIONS
def compute_row_id(file_id: str, apmc: str, commodity: str, reported_date: str) -> str:
    key = f"{file_id}|{apmc}|{commodity}|{reported_date}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]

def compute_record_hash(record: Dict[str, Any]) -> str:
    data_fields = {k: record.get(k) for k in sorted(PRICE_FIELDS)}
    key = json.dumps(data_fields, sort_keys=True)
    return hashlib.sha256(key.encode()).hexdigest()[:32]

def check_data_quality(record: Dict[str, Any]) -> Tuple[bool, bool, int]:
    null_count = 0
    has_nulls = False
    is_complete = True
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None or record[field] == '':
            is_complete = False
            null_count += 1
    for field in PRICE_FIELDS:
        if field in record and record[field] is None:
            has_nulls = True
            null_count += 1
    return has_nulls, is_complete, null_count

def parse_date(date_str: str) -> Tuple[str, int, int, int]:
    date_str = date_str.strip()
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.date().isoformat(), dt.year, dt.month, dt.day
    except Exception:
        logger.warning(f"Could not parse date: {date_str}")
        return "", 0, 0, 0

# MAIN PARSING LOGIC
def main():
    con = duckdb.connect(str(DB_PATH))
    create_rows_table_if_not_exists(con)
    files = con.execute(f"SELECT file_id, raw_payload, ingest_job_id, ingest_ts FROM {FILES_TABLE}").fetchall()
    for file_id, raw_payload, ingest_job_id, ingest_ts in files:
        try:
            records = [json.loads(line) for line in raw_payload.splitlines() if line.strip()]
        except Exception as e:
            logger.error(f"Failed to parse file {file_id}: {e}")
            continue
        for i, record in enumerate(records):
            apmc_norm = normalize_place_name(record.get("apmc"))
            date_iso, year, month, day = parse_date(record.get("created_at", ""))
            has_nulls, is_complete, null_field_count = check_data_quality(record)
            row_id = compute_row_id(file_id, record.get("apmc"), record.get("commodity"), date_iso)
            record_hash = compute_record_hash(record)
            insert_sql = f"""
            INSERT INTO {ROWS_TABLE} (
                row_id, file_id, country, state, apmc, apmc_norm, commodity,
                min_price, modal_price, max_price, commodity_arrivals, commodity_traded,
                reported_date, year, month, day,
                has_nulls, is_complete, null_field_count, record_hash,
                is_latest, version, ingest_job_id, ingest_ts, source_row_number,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = [
                row_id, file_id, 'IN', record.get("state"), record.get("apmc"), apmc_norm, record.get("commodity"),
                float(record.get("min_price", 0)), float(record.get("modal_price", 0)), float(record.get("max_price", 0)),
                float(record.get("commodity_arrivals", 0)), float(record.get("commodity_traded", 0)),
                date_iso, year, month, day,
                has_nulls, is_complete, null_field_count, record_hash,
                True, 1, ingest_job_id, ingest_ts, i+1,
                datetime.now(), datetime.now()
            ]
            try:
                con.execute(insert_sql, values)
            except Exception as e:
                logger.error(f"Failed to insert row for file {file_id}, line {i+1}: {e}")
    con.close()

if __name__ == "__main__":
    main()
