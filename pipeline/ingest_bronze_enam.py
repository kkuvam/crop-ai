#!/usr/bin/env python3
"""
pipeline/ingest_bronze_enam.py

Ingest eNAM JSON/JSONL files into DuckDB table `enam_bronze_files`.
- The data in DuckDB is partitioned by market (apmc), state, and commodity
- Parses filenames and/or file content to extract country, state, apmc, commodity, and other metadata
- Computes checksum and row_count (streaming for .jsonl)
- Normalizes market_name for partitioning (market_name_norm)
- Skips files already ingested (by checksum)
- Emits a small run manifest JSON per run under manifests/run_manifests/

SCHEMA:
--------
CREATE TABLE IF NOT EXISTS enam_bronze_files (
    -- Primary identification
    file_id VARCHAR PRIMARY KEY,
    checksum VARCHAR NOT NULL UNIQUE,
    original_filename VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    file_size_bytes BIGINT,
    
    -- Geographic metadata (parsed from filename)
    country VARCHAR DEFAULT 'IN',
    state_name VARCHAR,
    apmc_raw VARCHAR,
    from pipeline.ingest_bronze_common import normalize_place_name, file_checksum, count_jsonl_rows, extract_year_month_from_path

    apmc_norm VARCHAR,
    commodity_name VARCHAR,
    
    -- Temporal metadata
    year INTEGER,                                   -- data year
    month INTEGER,                                  -- data month (1-12)
    reported_date DATE,                             -- reported date (if available)

    -- Content metadata
    raw_payload VARCHAR,                            -- full JSON/JSONL content
    row_count INTEGER DEFAULT 1,                    -- number of data rows
    data_format VARCHAR,                            -- 'json' or 'jsonl'

    -- Partitioning hint
    partition_path VARCHAR,                         -- suggested partition path

    -- Ingestion tracking
    ingest_job_id VARCHAR NOT NULL,                 -- run ID for this ingestion
    ingest_ts TIMESTAMP NOT NULL,                   -- when file was ingested
    ingest_duration_ms INTEGER,                     -- how long ingestion took

    -- Audit timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_checksum_enam ON enam_bronze_files(checksum);
CREATE INDEX IF NOT EXISTS idx_apmc_norm_enam ON enam_bronze_files(apmc_norm);
CREATE INDEX IF NOT EXISTS idx_year_month_enam ON enam_bronze_files(year, month);
CREATE INDEX IF NOT EXISTS idx_ingest_job_enam ON enam_bronze_files(ingest_job_id);
CREATE INDEX IF NOT EXISTS idx_created_at_enam ON enam_bronze_files(created_at_ts);
"""
import os
import re
import json
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, Set
import duckdb
from pipeline.ingest_bronze_common import normalize_place_name, file_checksum, count_jsonl_rows, extract_year_month_from_path, get_existing_checksums


# CONFIGURATION
DATA_DIR = Path("/Volumes/Extreme/Mission/data/enam")
DB_PATH = Path("/Volumes/Extreme/Mission/duckdb/cropai.duckdb")
TABLE = "enam_bronze_files"
MANIFEST_DIR = Path("/Volumes/Extreme/Mission/manifests/run")
CHUNK_SIZE = 65536
MAX_PAYLOAD_SIZE = 100_000_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE SETUP
# ============================================================================
def create_table_if_not_exists(con: duckdb.DuckDBPyConnection) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        file_id VARCHAR PRIMARY KEY,
        checksum VARCHAR NOT NULL UNIQUE,
        original_filename VARCHAR NOT NULL,
        file_path VARCHAR NOT NULL,
        file_size_bytes BIGINT,
        country VARCHAR DEFAULT 'IN',
        state_name VARCHAR,
        apmc_raw VARCHAR,
        apmc_norm VARCHAR,
        commodity_name VARCHAR,
        year INTEGER,
        month INTEGER,
        reported_date DATE,
        raw_payload VARCHAR,
        row_count INTEGER DEFAULT 1,
        data_format VARCHAR,
        partition_path VARCHAR,
        ingest_job_id VARCHAR NOT NULL,
        ingest_ts TIMESTAMP NOT NULL,
        ingest_duration_ms INTEGER,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_checksum_enam ON {TABLE}(checksum);",
        f"CREATE INDEX IF NOT EXISTS idx_apmc_norm_enam ON {TABLE}(apmc_norm);",
        f"CREATE INDEX IF NOT EXISTS idx_year_month_enam ON {TABLE}(year, month);",
        f"CREATE INDEX IF NOT EXISTS idx_ingest_job_enam ON {TABLE}(ingest_job_id);",
        f"CREATE INDEX IF NOT EXISTS idx_created_at_enam ON {TABLE}(created_at_ts);",
    ]
    try:
        con.execute(create_table_sql)
        logger.info(f"Table '{TABLE}' ready")
        for idx_sql in indexes:
            con.execute(idx_sql)
        logger.info("All indexes created")
    except Exception as e:
        logger.error(f"Failed to create table/indexes: {e}")
        raise

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def build_partition_path(apmc: Optional[str], state: Optional[str], year: int, month: int) -> str:
    apmc_norm = normalize_place_name(apmc) if apmc else "unknown_apmc"
    state_norm = normalize_place_name(state) if state else "unknown_state"
    return f"apmc={apmc_norm}/state={state_norm}/year={year}/month={month:02d}"

def parse_file_metadata(fpath: Path) -> Dict[str, Any]:
    start_time = time.time()
    fname = fpath.name
    file_size = fpath.stat().st_size
    data_format = 'jsonl' if fname.endswith('.jsonl') else 'json'
    year, month = extract_year_month_from_path(fpath)
    checksum = file_checksum(fpath)
    raw_payload = None
    row_count = 1
    records = []
    if file_size > MAX_PAYLOAD_SIZE:
        logger.warning(f"File too large ({file_size} bytes), skipping payload: {fpath}")
        raw_payload = f"[PAYLOAD_TOO_LARGE: {file_size} bytes]"
    else:
        try:
            if data_format == 'jsonl':
                row_count = count_jsonl_rows(fpath)
                with fpath.open('r', encoding='utf-8') as fh:
                    raw_payload = fh.read()
                    fh.seek(0)
                    for line in fh:
                        try:
                            records.append(json.loads(line))
                        except Exception:
                            continue
            else:
                with fpath.open('r', encoding='utf-8') as fh:
                    text = fh.read()
                try:
                    j = json.loads(text)
                    if isinstance(j, list):
                        records = j
                        row_count = len(j)
                    else:
                        records = [j]
                        row_count = 1
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {fpath}, treating as single row")
                    records = []
                    row_count = 1
                raw_payload = text
        except Exception as e:
            logger.error(f"Failed to read payload from {fpath}: {e}")
            raw_payload = f"[READ_ERROR: {str(e)}]"

    # Extract metadata from first record (if available)
    meta = records[0] if records else {}
    state_name = meta.get("state")
    apmc_raw = meta.get("apmc")
    apmc_norm = normalize_place_name(apmc_raw)
    commodity_name = meta.get("commodity")
    # ...existing code...
    created_at = meta.get("created_at")
    status = meta.get("status")
    commodity_uom = meta.get("Commodity_Uom")

    partition_path = build_partition_path(apmc_raw, state_name, year, month)
    duration_ms = int((time.time() - start_time) * 1000)

    return {
        'file_id': checksum,
        'checksum': checksum,
        'original_filename': fname,
        'file_path': str(fpath.resolve()),
        'file_size_bytes': file_size,
        'country': 'IN',
        'state_name': state_name,
        'apmc_raw': apmc_raw,
        'apmc_norm': apmc_norm,
        'commodity_name': commodity_name,
        'created_at': created_at,
        'status': status,
        'commodity_uom': commodity_uom,
        'year': year,
        'month': month,
        'raw_payload': raw_payload,
        'row_count': row_count,
        'data_format': data_format,
        'partition_path': partition_path,
        'ingest_duration_ms': duration_ms
    }


def main():
    try:
        con = duckdb.connect(str(DB_PATH))
        logger.info(f"Connected to DuckDB at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        return
    try:
        create_table_if_not_exists(con)
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {e}")
        con.close()
        return
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"bronze_enam_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    manifest = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "ingested_files": [],
        "skipped_files": [],
        "errors": [],
        "stats": {
            "total_files_scanned": 0,
            "files_ingested": 0,
            "files_skipped": 0,
            "files_errored": 0,
            "total_rows_ingested": 0
        }
    }
    insert_sql = f"""
    INSERT INTO {TABLE} (
        file_id, checksum, original_filename, file_path, file_size_bytes,
        country, state_name, apmc_raw, apmc_norm, commodity_name,
        created_at, status, commodity_uom, year, month, raw_payload, row_count, data_format,
        partition_path, ingest_job_id, ingest_ts, ingest_duration_ms, created_at_ts, updated_at_ts
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
    existing_checksums = get_existing_checksums(con, TABLE)
    logger.info(f"Found {len(existing_checksums)} existing files in database")
    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        con.close()
        return
    logger.info(f"Scanning files in {DATA_DIR}")
    for root, dirs, files in os.walk(DATA_DIR):
        for fname in sorted(files):
            if not (fname.endswith('.json') or fname.endswith('.jsonl')):
                continue
            fpath = Path(root) / fname
            manifest["stats"]["total_files_scanned"] += 1
            try:
                metadata = parse_file_metadata(fpath)
                checksum = metadata['checksum']
                if checksum in existing_checksums:
                    logger.info(f"Skipping (already ingested): {fpath.name}")
                    manifest["skipped_files"].append({
                        "path": str(fpath),
                        "file_id": checksum,
                        "reason": "already_ingested"
                    })
                    manifest["stats"]["files_skipped"] += 1
                    continue
                now = datetime.now()
                con.execute(insert_sql, [
                    metadata['file_id'],
                    metadata['checksum'],
                    metadata['original_filename'],
                    metadata['file_path'],
                    metadata['file_size_bytes'],
                    metadata['country'],
                    metadata['state_name'],
                    metadata['apmc_raw'],
                    metadata['apmc_norm'],
                    metadata['commodity_name'],
                # ...existing code...
                    metadata['created_at'],
                    metadata['status'],
                    metadata['commodity_uom'],
                    metadata['year'],
                    metadata['month'],
                    metadata['raw_payload'],
                    metadata['row_count'],
                    metadata['data_format'],
                    metadata['partition_path'],
                    run_id,
                    now,
                    metadata['ingest_duration_ms'],
                    now,
                    now
                ])
                existing_checksums.add(checksum)
                manifest["ingested_files"].append({
                    "path": str(fpath),
                    "file_id": metadata['file_id'],
                    "rows": metadata['row_count'],
                    "partition_path": metadata['partition_path'],
                    "duration_ms": metadata['ingest_duration_ms']
                })
                manifest["stats"]["files_ingested"] += 1
                manifest["stats"]["total_rows_ingested"] += metadata['row_count']
                logger.info(
                    f"✓ Ingested: {fpath.name} | "
                    f"rows={metadata['row_count']} | "
                    f"duration={metadata['ingest_duration_ms']}ms"
                )
            except Exception as e:
                logger.exception(f"Failed to ingest {fpath}")
                manifest["errors"].append({
                    "path": str(fpath),
                    "error": str(e),
                    "error_type": type(e).__name__
                })
                manifest["stats"]["files_errored"] += 1
    manifest["ended_at"] = datetime.now().isoformat()
    manifest_path = MANIFEST_DIR / f"manifest_{run_id}.json"
    try:
        with manifest_path.open("w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2, default=str)
        logger.info(f"Manifest written to {manifest_path}")
    except Exception as e:
        logger.error(f"Failed to write manifest: {e}")
    stats = manifest["stats"]
    logger.info("=" * 70)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total files scanned:    {stats['total_files_scanned']}")
    logger.info(f"Files ingested:         {stats['files_ingested']}")
    logger.info(f"Files skipped:          {stats['files_skipped']}")
    logger.info(f"Files with errors:      {stats['files_errored']}")
    logger.info(f"Total rows ingested:    {stats['total_rows_ingested']}")
    logger.info("=" * 70)
    con.close()
    logger.info("Database connection closed")

if __name__ == "__main__":
    main()
