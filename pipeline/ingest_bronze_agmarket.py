#!/usr/bin/env python3
"""
pipeline/ingest_bronze_agmarket.py

Ingest Agmarknet JSON/JSONL files into DuckDB table `agmarknet_bronze_files`.
- The data in DuckDB is partitioned by market_name, state, and commodity
- Parses filenames and/or file content to extract country, state, district, market, commodity, and other metadata
- Computes checksum and row_count (streaming for .jsonl)
- Normalizes market_name for partitioning (market_name_norm)
- Skips files already ingested (by checksum)
- Emits a small run manifest JSON per run under manifests/run_manifests/

IMPROVED SCHEMA:
----------------

CREATE TABLE IF NOT EXISTS agmarknet_bronze_files (
    -- Primary identification
    file_id VARCHAR PRIMARY KEY,                    -- checksum-based unique ID
    checksum VARCHAR NOT NULL UNIQUE,               -- SHA256 checksum for deduplication
    original_filename VARCHAR NOT NULL,             -- original file name
    file_path VARCHAR NOT NULL,                     -- full resolved path
    file_size_bytes BIGINT,                         -- file size for monitoring

    -- Metadata (parsed from filename/content)
    country VARCHAR DEFAULT 'IN',                   -- e.g., 'IN' for India
    state_name VARCHAR,                             -- e.g., 'Madhya Pradesh'
    district_name VARCHAR,                          -- e.g., 'Umariya'
    market_name_raw VARCHAR,                      -- original market name
    market_name_norm VARCHAR,                     -- normalized market name
    commodity_name VARCHAR,                         -- e.g., 'Wheat'
    commodity_group VARCHAR,                        -- commodity group

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

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_checksum ON agmarknet_bronze_files(checksum);
CREATE INDEX IF NOT EXISTS idx_market_norm ON agmarknet_bronze_files(market_name);
CREATE INDEX IF NOT EXISTS idx_year_month ON agmarknet_bronze_files(year, month);
CREATE INDEX IF NOT EXISTS idx_ingest_job ON agmarknet_bronze_files(ingest_job_id);
CREATE INDEX IF NOT EXISTS idx_created_at ON agmarknet_bronze_files(created_at);
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

# ============================================================================
# CONFIGURATION
# ============================================================================
DATA_DIR = Path("/Volumes/Extreme/Mission/data/agmarket")
DB_PATH = Path("/Volumes/Extreme/Mission/duckdb/cropai.duckdb")
TABLE = "agmarknet_bronze_files"
MANIFEST_DIR = Path("/Volumes/Extreme/Mission/manifests/run")

CHUNK_SIZE = 65536              # 64KB chunks for file reading
MAX_PAYLOAD_SIZE = 100_000_000  # 100MB max payload (safety limit)

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Regex patterns
FLOAT_FIND_RE = re.compile(r"-?\d+\.\d+")

# ============================================================================
# DATABASE SETUP
# ============================================================================
def create_table_if_not_exists(con: duckdb.DuckDBPyConnection) -> None:
    """Create the bronze files table with optimized schema if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        file_id VARCHAR PRIMARY KEY,
        checksum VARCHAR NOT NULL UNIQUE,
        original_filename VARCHAR NOT NULL,
        file_path VARCHAR NOT NULL,
        file_size_bytes BIGINT,
        country VARCHAR DEFAULT 'IN',
        state_name VARCHAR,
        district_name VARCHAR,
        market_name_raw VARCHAR,
        market_name_norm VARCHAR,
        commodity_name VARCHAR,
        commodity_group VARCHAR,
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
        f"CREATE INDEX IF NOT EXISTS idx_checksum ON {TABLE}(checksum);",
        f"CREATE INDEX IF NOT EXISTS idx_market_norm ON {TABLE}(market_name);",
        f"CREATE INDEX IF NOT EXISTS idx_year_month ON {TABLE}(year, month);",
        f"CREATE INDEX IF NOT EXISTS idx_ingest_job ON {TABLE}(ingest_job_id);",
        f"CREATE INDEX IF NOT EXISTS idx_created_at ON {TABLE}(created_at);",
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

def build_partition_path(
    market_name: Optional[str],
    state_name: Optional[str],
    district_name: Optional[str],
    year: int,
    month: int
) -> str:
    """Build partition path using market, state, district, year, and month."""
    market_norm = normalize_place_name(market_name) if market_name else "unknown_market"
    state_norm = normalize_place_name(state_name) if state_name else "unknown_state"
    district_norm = normalize_place_name(district_name) if district_name else "unknown_district"
    return f"market={market_norm}/state={state_norm}/district={district_norm}/year={year}/month={month:02d}"


def parse_file_metadata(fpath: Path) -> Dict[str, Any]:
    """
    Parse all metadata from a single file.
    Returns dict with all fields ready for database insertion.
    """
    start_time = time.time()
    
    # Basic file info
    fname = fpath.name
    base = fname.rsplit('.', 1)[0]
    parts = base.split('_')
    # Basic file info
    fname = fpath.name
    file_size = fpath.stat().st_size
    data_format = 'jsonl' if fname.endswith('.jsonl') else 'json'

    # Only extract year, month, and day from file path/filename
    year, month = extract_year_month_from_path(fpath)
    # Optionally extract day if needed
    reported_date = None
    # Try to extract day from filename (e.g., YYYYMMDD or YYYY-MM-DD)
    m = re.search(r'(\d{4})[-_]?([01]\d)[-_]?([0-3]\d)', fname)
    if m:
        try:
            reported_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        except Exception:
            reported_date = None

    # Checksum
    checksum = file_checksum(fpath)

    # Read payload and count rows
    raw_payload = None
    row_count = 1

    if file_size > MAX_PAYLOAD_SIZE:
        logger.warning(f"File too large ({file_size} bytes), skipping payload: {fpath}")
        raw_payload = f"[PAYLOAD_TOO_LARGE: {file_size} bytes]"
    else:
        try:
            if data_format == 'jsonl':
                row_count = count_jsonl_rows(fpath)
                with fpath.open('r', encoding='utf-8') as fh:
                    raw_payload = fh.read()
            else:  # json
                with fpath.open('r', encoding='utf-8') as fh:
                    text = fh.read()
                try:
                    j = json.loads(text)
                    row_count = len(j) if isinstance(j, list) else 1
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {fpath}, treating as single row")
                    row_count = 1
                raw_payload = text
        except Exception as e:
            logger.error(f"Failed to read payload from {fpath}: {e}")
            raw_payload = f"[READ_ERROR: {str(e)}]"

    # Partition path: just year/month (no market/state/district)
    partition_path = f"year={year}/month={month:02d}"
    duration_ms = int((time.time() - start_time) * 1000)

    return {
        'file_id': checksum,
        'checksum': checksum,
        'original_filename': fname,
        'file_path': str(fpath.resolve()),
        'file_size_bytes': file_size,
        'country': None,
        'state_name': None,
        'district_name': None,
        'market_name_raw': None,
        'market_name_norm': None,
        'commodity_name': None,
        'commodity_group': None,
        'year': year,
        'month': month,
        'reported_date': reported_date,
        'raw_payload': raw_payload,
        'row_count': row_count,
        'data_format': data_format,
        'partition_path': partition_path,
        'ingest_duration_ms': duration_ms
    }


# ============================================================================
# MAIN INGESTION LOGIC
# ============================================================================
def main():
    """Main ingestion pipeline."""
    
    # Initialize database connection
    try:
        con = duckdb.connect(str(DB_PATH))
        logger.info(f"Connected to DuckDB at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        return
    
    # Create table and indexes
    try:
        create_table_if_not_exists(con)
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {e}")
        con.close()
        return
    
    # Prepare manifest
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"bronze_agmarknet_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
    
    # Prepare SQL insert statement
    insert_sql = f"""
    INSERT INTO {TABLE} (
        file_id, checksum, original_filename, file_path, file_size_bytes,
        country, state_name, district_name, market_name_raw, market_name_norm, commodity_name, commodity_group,
        year, month, reported_date,
        raw_payload, row_count, data_format,
        partition_path, ingest_job_id, ingest_ts, ingest_duration_ms,
        created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?
    )
    """
    
            
    # Get existing checksums for deduplication
    existing_checksums = get_existing_checksums(con, TABLE)
    logger.info(f"Found {len(existing_checksums)} existing files in database")
    
    # Validate data directory
    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        con.close()
        return
    
    # Walk through files
    logger.info(f"Scanning files in {DATA_DIR}")
    
    for root, dirs, files in os.walk(DATA_DIR):
        for fname in sorted(files):
            if not (fname.endswith('.json') or fname.endswith('.jsonl')):
                continue
            
            fpath = Path(root) / fname
            manifest["stats"]["total_files_scanned"] += 1
            
            try:
                # Parse file metadata
                metadata = parse_file_metadata(fpath)
                checksum = metadata['checksum']
                
                # Check if already ingested
                if checksum in existing_checksums:
                    logger.info(f"Skipping (already ingested): {fpath.name}")
                    manifest["skipped_files"].append({
                        "path": str(fpath),
                        "file_id": checksum,
                        "reason": "already_ingested"
                    })
                    manifest["stats"]["files_skipped"] += 1
                    continue
                
                # Insert into database
                now = datetime.now()
                con.execute(insert_sql, [
                    metadata['file_id'],
                    metadata['checksum'],
                    metadata['original_filename'],
                    metadata['file_path'],
                    metadata['file_size_bytes'],
                    metadata['country'],
                    metadata['state_name'],
                    metadata['district_name'],
                    metadata['market_name_raw'],
                    metadata['market_name_norm'],
                    metadata['commodity_name'],
                    metadata['commodity_group'],
                    metadata['year'],
                    metadata['month'],
                    metadata['reported_date'],
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
    
    # Finalize manifest
    manifest["ended_at"] = datetime.now().isoformat()
    manifest_path = MANIFEST_DIR / f"manifest_{run_id}.json"
    
    try:
        with manifest_path.open("w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2, default=str)
        logger.info(f"Manifest written to {manifest_path}")
    except Exception as e:
        logger.error(f"Failed to write manifest: {e}")
    
    # Summary
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