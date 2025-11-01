#!/usr/bin/env python3
"""
pipeline/parse_bronze_agmarket.py

Parse Agmarknet data from `agmarknet_bronze_files` table into `agmarknet_bronze_rows`.
- Reads raw_payload from bronze_files table
- Expands JSON/JSONL into individual market arrival records
- Computes deterministic row_id and record_hash for deduplication
- Handles versioning for reloads (increments version, maintains is_latest)
- Tracks data quality (nulls, completeness)
- Emits run manifest for each parsing job

SCHEMA:
-------
CREATE TABLE IF NOT EXISTS agmarknet_bronze_rows (
    row_id VARCHAR PRIMARY KEY,
    file_id VARCHAR NOT NULL,
    country VARCHAR DEFAULT 'IN',
    state_name VARCHAR,
    district_name VARCHAR,
    market_name VARCHAR,
    market_name_norm VARCHAR,
    commodity_name VARCHAR,
    commodity_group VARCHAR,
    variety VARCHAR,
    grade VARCHAR,
    reported_date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    arrivals DOUBLE,
    min_price DOUBLE,
    max_price DOUBLE,
    modal_price DOUBLE,
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
    FOREIGN KEY (file_id) REFERENCES agmarknet_bronze_files(file_id)
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

# ============================================================================
# CONFIGURATION
# ============================================================================
DB_PATH = Path("/Volumes/Extreme/Mission/duckdb/cropai.duckdb")
FILES_TABLE = "agmarknet_bronze_files"
ROWS_TABLE = "agmarknet_bronze_rows"
MANIFEST_DIR = Path("/Volumes/Extreme/Mission/manifests/run")

# Expected fields for quality checks
REQUIRED_FIELDS = [
    "state_name", "district_name", "market_name", "commodity_name",
    "reported_date"
]

PRICE_FIELDS = ["arrivals", "min_price", "max_price", "modal_price"]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================================
# DATABASE SETUP
# ============================================================================
def create_rows_table_if_not_exists(con: duckdb.DuckDBPyConnection) -> None:
    """Create the bronze rows table with optimized schema if it doesn't exist."""
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {ROWS_TABLE} (
        -- Primary identification
        row_id VARCHAR PRIMARY KEY,
        file_id VARCHAR NOT NULL,
        
        -- Geographic identifiers
        country VARCHAR DEFAULT 'IN',
        state_name VARCHAR,
        district_name VARCHAR,
        market_name VARCHAR,
        market_name_norm VARCHAR,
        
        -- Commodity information
        commodity_name VARCHAR,
        commodity_group VARCHAR,
        variety VARCHAR,
        grade VARCHAR,
        
        -- Temporal dimension
        reported_date DATE NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        
        -- Market data
        arrivals DOUBLE,
        min_price DOUBLE,
        max_price DOUBLE,
        modal_price DOUBLE,
        
        -- Data quality flags
        has_nulls BOOLEAN DEFAULT FALSE,
        is_complete BOOLEAN DEFAULT TRUE,
        null_field_count INTEGER DEFAULT 0,
        
        -- Deduplication & versioning
        record_hash VARCHAR NOT NULL,
        is_latest BOOLEAN DEFAULT TRUE,
        version INTEGER DEFAULT 1,
        superseded_by VARCHAR,
        superseded_at TIMESTAMP,
        
        -- Ingestion tracking
        ingest_job_id VARCHAR NOT NULL,
        ingest_ts TIMESTAMP NOT NULL,
        source_row_number INTEGER,
        
        -- Audit timestamps
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        
        -- Foreign key
        FOREIGN KEY (file_id) REFERENCES {FILES_TABLE}(file_id)
    );
    """
    
    # Create indexes for performance
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_rows_file_id ON {ROWS_TABLE}(file_id);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_date ON {ROWS_TABLE}(reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_market_date ON {ROWS_TABLE}(market_name_norm, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_commodity_date ON {ROWS_TABLE}(commodity_name, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_state_date ON {ROWS_TABLE}(state_name, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_year_month ON {ROWS_TABLE}(year, month);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_ingest_job ON {ROWS_TABLE}(ingest_job_id);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_record_hash ON {ROWS_TABLE}(record_hash);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_latest ON {ROWS_TABLE}(is_latest) WHERE is_latest = TRUE;",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_rows_unique_market_commodity_date_version ON {ROWS_TABLE}(market_name, commodity_name, variety, grade, reported_date, version);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_created_at ON {ROWS_TABLE}(created_at);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_superseded ON {ROWS_TABLE}(superseded_by, superseded_at) WHERE superseded_by IS NOT NULL;",
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


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def compute_row_id(
    file_id: str,
    market_name: str,
    commodity_name: str,
    variety: str,
    grade: str,
    reported_date: str
) -> str:
    """Generate deterministic row_id from key fields."""
    key = f"{file_id}|{market_name}|{commodity_name}|{variety or ''}|{grade or ''}|{reported_date}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def compute_record_hash(record: Dict[str, Any]) -> str:
    """Generate hash of market data (excludes metadata)."""
    # Extract only data fields in consistent order
    data_fields = {
        k: record.get(k) 
        for k in sorted(PRICE_FIELDS)
    }
    key = json.dumps(data_fields, sort_keys=True)
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def check_data_quality(record: Dict[str, Any]) -> Tuple[bool, bool, int]:
    """
    Check data quality of a market record.
    
    Returns:
        (has_nulls, is_complete, null_field_count)
    """
    null_count = 0
    has_nulls = False
    is_complete = True
    
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None or record[field] == '':
            is_complete = False
            null_count += 1
    
    # Check price fields for nulls
    for field in PRICE_FIELDS:
        if field in record and record[field] is None:
            has_nulls = True
            null_count += 1
    
    return has_nulls, is_complete, null_count


def parse_date(date_str: str) -> Tuple[str, int, int, int]:
    """
    Parse date string in format 'DD MMM YYYY' (e.g., '01 Jan 2025').
    
    Returns:
        (date_iso, year, month, day)
    """
    # Handle various date formats
    date_str = date_str.strip()
    
    # Try 'DD MMM YYYY' format (e.g., '01 Jan 2025')
    try:
        dt = datetime.strptime(date_str, '%d %b %Y')
        return dt.strftime('%Y-%m-%d'), dt.year, dt.month, dt.day
    except ValueError:
        pass
    
    # Try 'DD-MMM-YYYY' format
    try:
        dt = datetime.strptime(date_str, '%d-%b-%Y')
        return dt.strftime('%Y-%m-%d'), dt.year, dt.month, dt.day
    except ValueError:
        pass
    
    # Try 'YYYY-MM-DD' format
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%Y-%m-%d'), dt.year, dt.month, dt.day
    except ValueError:
        pass
    
    # Try 'DD/MM/YYYY' format
    try:
        dt = datetime.strptime(date_str, '%d/%m/%Y')
        return dt.strftime('%Y-%m-%d'), dt.year, dt.month, dt.day
    except ValueError:
        pass
    
    raise ValueError(f"Could not parse date: {date_str}")


def parse_numeric(value: Any) -> Optional[float]:
    """Parse numeric value, handling strings and null values."""
    if value is None or value == '':
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Remove commas and whitespace
        value = value.replace(',', '').strip()
        if value == '' or value.lower() in ('na', 'n/a', 'null', 'none'):
            return None
        try:
            return float(value)
        except ValueError:
            return None
    
    return None


def parse_json_payload(payload: str, data_format: str) -> List[Dict[str, Any]]:
    """
    Parse raw_payload into list of records.
    
    Args:
        payload: Raw JSON/JSONL string
        data_format: 'json' or 'jsonl'
    
    Returns:
        List of market records
    """
    records = []
    
    if data_format == 'jsonl':
        for line in payload.strip().split('\n'):
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSONL line: {e}")
    else:  # json
        try:
            data = json.loads(payload)
            if isinstance(data, list):
                records = data
            else:
                records = [data]
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON payload: {e}")
    
    return records


def get_existing_row_versions(
    con: duckdb.DuckDBPyConnection,
    market_name: str,
    commodity_name: str,
    variety: str,
    grade: str,
    reported_date: str
) -> List[Dict[str, Any]]:
    """
    Get all existing versions for a specific market/commodity/date combination.
    
    Returns:
        List of dicts with row_id, version, record_hash, is_latest
    """
    query = f"""
    SELECT row_id, version, record_hash, is_latest
    FROM {ROWS_TABLE}
    WHERE market_name = ? 
      AND commodity_name = ? 
      AND COALESCE(variety, '') = ?
      AND COALESCE(grade, '') = ?
      AND reported_date = ?
    ORDER BY version DESC
    """
    
    try:
        result = con.execute(query, [
            market_name,
            commodity_name,
            variety or '',
            grade or '',
            reported_date
        ]).fetchall()
        
        return [
            {
                'row_id': r[0],
                'version': r[1],
                'record_hash': r[2],
                'is_latest': r[3]
            }
            for r in result
        ]
    except Exception as e:
        logger.warning(f"Error fetching existing versions: {e}")
        return []


def process_file_records(
    con: duckdb.DuckDBPyConnection,
    file_record: Dict[str, Any],
    run_id: str,
    manifest: Dict[str, Any]
) -> int:
    """
    Process all records from a single file and insert into rows table.
    
    Returns:
        Number of rows inserted
    """
    file_id = file_record['file_id']
    raw_payload = file_record['raw_payload']
    data_format = file_record['data_format']
    
    # Skip files with read errors
    if raw_payload.startswith('[READ_ERROR:') or raw_payload.startswith('[PAYLOAD_TOO_LARGE:'):
        logger.warning(f"Skipping file {file_id}: {raw_payload[:100]}")
        manifest['skipped_files'].append({
            'file_id': file_id,
            'reason': 'payload_error'
        })
        return 0
    
    # Parse payload
    records = parse_json_payload(raw_payload, data_format)
    if not records:
        logger.warning(f"No records found in file {file_id}")
        manifest['skipped_files'].append({
            'file_id': file_id,
            'reason': 'no_records'
        })
        return 0
    
    # Prepare insert SQL
    insert_sql = f"""
    INSERT INTO {ROWS_TABLE} (
        row_id, file_id, country,
        state_name, district_name, market_name, market_name_norm,
        commodity_name, commodity_group, variety, grade,
        reported_date, year, month, day,
        arrivals, min_price, max_price, modal_price,
        has_nulls, is_complete, null_field_count,
        record_hash, is_latest, version,
        ingest_job_id, ingest_ts, source_row_number,
        created_at, updated_at
    ) VALUES (
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?
    )
    """
    
    update_superseded_sql = f"""
    UPDATE {ROWS_TABLE}
    SET is_latest = FALSE,
        superseded_by = ?,
        superseded_at = ?,
        updated_at = ?
    WHERE row_id = ?
    """
    
    rows_inserted = 0
    rows_skipped = 0
    rows_updated = 0
    now = datetime.now()
    
    for idx, record in enumerate(records):
        try:
            # Validate required fields
            if 'reported_date' not in record or not record['reported_date']:
                logger.warning(f"Record {idx} missing 'reported_date' field in file {file_id}")
                continue
            
            # Parse date
            try:
                date_iso, year, month, day = parse_date(record['reported_date'])
            except ValueError as e:
                logger.warning(f"Record {idx} has invalid date in file {file_id}: {e}")
                continue
            
            # Extract fields
            state_name = record.get('state_name', '').strip() or None
            district_name = record.get('district_name', '').strip() or None
            market_name = record.get('market_name', '').strip() or None
            commodity_name = record.get('commodity_name', '').strip() or None
            commodity_group = record.get('group', '').strip() or None
            variety = record.get('variety', '').strip() or None
            grade = record.get('grade', '').strip() or None
            
            # Skip if missing critical fields
            if not market_name or not commodity_name:
                logger.warning(f"Record {idx} missing market_name or commodity_name in file {file_id}")
                continue
            
            # Normalize market name
            market_name_norm = normalize_place_name(market_name)
            
            # Parse numeric fields
            arrivals = parse_numeric(record.get('arrivals'))
            min_price = parse_numeric(record.get('min_price'))
            max_price = parse_numeric(record.get('max_price'))
            modal_price = parse_numeric(record.get('modal_price'))
            
            # Generate IDs and hashes
            row_id = compute_row_id(
                file_id, market_name, commodity_name,
                variety or '', grade or '', date_iso
            )
            record_hash = compute_record_hash({
                'arrivals': arrivals,
                'min_price': min_price,
                'max_price': max_price,
                'modal_price': modal_price
            })
            
            # Check data quality
            has_nulls, is_complete, null_count = check_data_quality(record)
            
            # Check for existing versions
            existing = get_existing_row_versions(
                con, market_name, commodity_name,
                variety or '', grade or '', date_iso
            )
            
            version = 1
            is_new_version = False
            
            if existing:
                latest = existing[0]
                
                # Check if data has actually changed
                if latest['record_hash'] == record_hash:
                    # Data unchanged, skip
                    rows_skipped += 1
                    continue
                
                # Data changed, create new version
                version = latest['version'] + 1
                is_new_version = True
                
                # Mark old version as superseded
                con.execute(update_superseded_sql, [
                    row_id,
                    now,
                    now,
                    latest['row_id']
                ])
                rows_updated += 1
            
            # Insert new row
            con.execute(insert_sql, [
                row_id, file_id, 'IN',
                state_name, district_name, market_name, market_name_norm,
                commodity_name, commodity_group, variety, grade,
                date_iso, year, month, day,
                arrivals, min_price, max_price, modal_price,
                has_nulls, is_complete, null_count,
                record_hash, True, version,
                run_id, now, idx + 1,
                now, now
            ])
            
            rows_inserted += 1
            
            if is_new_version:
                logger.debug(
                    f"Created version {version} for {market_name}/{commodity_name} "
                    f"on {date_iso}"
                )
            
        except Exception as e:
            logger.error(f"Failed to process record {idx} from file {file_id}: {e}")
            manifest['errors'].append({
                'file_id': file_id,
                'record_index': idx,
                'error': str(e),
                'error_type': type(e).__name__
            })
    
    # Log summary for this file
    logger.info(
        f"File {file_id}: inserted={rows_inserted}, "
        f"skipped={rows_skipped}, updated={rows_updated}"
    )
    
    manifest['processed_files'].append({
        'file_id': file_id,
        'rows_inserted': rows_inserted,
        'rows_skipped': rows_skipped,
        'rows_updated': rows_updated
    })
    
    return rows_inserted


# ============================================================================
# MAIN PARSING LOGIC
# ============================================================================
def main():
    """Main parsing pipeline."""
    
    # Initialize database connection
    try:
        con = duckdb.connect(str(DB_PATH))
        logger.info(f"Connected to DuckDB at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        return
    
    # Create rows table and indexes
    try:
        create_rows_table_if_not_exists(con)
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {e}")
        con.close()
        return
    
    # Prepare manifest
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"parse_agmarket_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    manifest = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "processed_files": [],
        "skipped_files": [],
        "errors": [],
        "stats": {
            "files_processed": 0,
            "files_skipped": 0,
            "total_rows_inserted": 0,
            "total_rows_skipped": 0,
            "total_rows_updated": 0,
            "total_errors": 0
        }
    }
    
    # Fetch files that need parsing
    query = f"""
    SELECT 
        file_id, checksum, original_filename, file_path,
        country, state_name, district_name,
        market_name_raw, market_name_norm,
        commodity_name, commodity_group,
        year, month, raw_payload, row_count, data_format
    FROM {FILES_TABLE}
    WHERE raw_payload IS NOT NULL
    ORDER BY created_at
    """
    
    try:
        files = con.execute(query).fetchall()
        logger.info(f"Found {len(files)} files to process")
    except Exception as e:
        logger.error(f"Failed to fetch files: {e}")
        con.close()
        return
    
    # Process each file
    for file_row in files:
        file_record = {
            'file_id': file_row[0],
            'checksum': file_row[1],
            'original_filename': file_row[2],
            'file_path': file_row[3],
            'country': file_row[4],
            'state_name': file_row[5],
            'district_name': file_row[6],
            'market_name_raw': file_row[7],
            'market_name_norm': file_row[8],
            'commodity_name': file_row[9],
            'commodity_group': file_row[10],
            'year': file_row[11],
            'month': file_row[12],
            'raw_payload': file_row[13],
            'row_count': file_row[14],
            'data_format': file_row[15]
        }
        
        try:
            rows_inserted = process_file_records(con, file_record, run_id, manifest)
            manifest['stats']['total_rows_inserted'] += rows_inserted
            manifest['stats']['files_processed'] += 1
            
        except Exception as e:
            logger.exception(f"Failed to process file {file_record['file_id']}")
            manifest['errors'].append({
                'file_id': file_record['file_id'],
                'error': str(e),
                'error_type': type(e).__name__
            })
            manifest['stats']['files_skipped'] += 1
    
    # Finalize manifest
    manifest['ended_at'] = datetime.now().isoformat()
    manifest['stats']['total_errors'] = len(manifest['errors'])
    
    manifest_path = MANIFEST_DIR / f"manifest_{run_id}.json"
    
    try:
        with manifest_path.open("w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2, default=str)
        logger.info(f"Manifest written to {manifest_path}")
    except Exception as e:
        logger.error(f"Failed to write manifest: {e}")
    
    # Summary
    stats = manifest['stats']
    logger.info("=" * 70)
    logger.info("PARSING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Files processed:        {stats['files_processed']}")
    logger.info(f"Files skipped:          {stats['files_skipped']}")
    logger.info(f"Total rows inserted:    {stats['total_rows_inserted']}")
    logger.info(f"Total errors:           {stats['total_errors']}")
    logger.info("=" * 70)
    
    con.close()
    logger.info("Database connection closed")


if __name__ == "__main__":
    main()