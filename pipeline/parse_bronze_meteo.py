#!/usr/bin/env python3
"""
pipeline/parse_bronze_meteo.py

Parse Open-Meteo data from `openmeteo_bronze_files` table into `openmeteo_bronze_rows`.
- Reads raw_payload from bronze_files table
- Expands JSON/JSONL into individual daily weather records
- Computes deterministic row_id and record_hash for deduplication
- Handles versioning for reloads (increments version, maintains is_latest)
- Tracks data quality (nulls, completeness)
- Emits run manifest for each parsing job

SCHEMA:
-------
CREATE TABLE IF NOT EXISTS openmeteo_bronze_rows (
    -- Primary identifiers
    row_id VARCHAR PRIMARY KEY,
    file_id VARCHAR NOT NULL,
    
    -- Geographic identifiers
    country VARCHAR DEFAULT 'IN',
    state VARCHAR,
    district VARCHAR,
    place_name_norm VARCHAR,
    
    -- Coordinates
    lat DOUBLE,
    lon DOUBLE,
    lat_tile DOUBLE,
    lon_tile DOUBLE,
    
    --Temodal metadata
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    reported_date DATE NOT NULL,
    
    -- Content fields
    temperature_2m_mean DOUBLE,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    cloud_cover_mean DOUBLE,
    relative_humidity_2m_max DOUBLE,
    relative_humidity_2m_min DOUBLE,
    relative_humidity_2m_mean DOUBLE,
    wind_speed_10m_max DOUBLE,
    wind_speed_10m_min DOUBLE,
    wind_speed_10m_mean DOUBLE,
    wet_bulb_temperature_2m_min DOUBLE,
    wet_bulb_temperature_2m_max DOUBLE,
    wet_bulb_temperature_2m_mean DOUBLE,
    wind_direction_10m_dominant DOUBLE,
    rain_sum DOUBLE,
    precipitation_sum DOUBLE,
    
    -- Data quality flags
    has_nulls BOOLEAN DEFAULT FALSE,
    is_complete BOOLEAN DEFAULT TRUE,
    null_field_count INTEGER DEFAULT 0,
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
    FOREIGN KEY (file_id) REFERENCES openmeteo_bronze_files(file_id)
);
"""

import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Tuple
import duckdb

# ============================================================================
# CONFIGURATION
# ============================================================================
DB_PATH = Path("/Volumes/Extreme/Mission/duckdb/cropai.duckdb")
FILES_TABLE = "openmeteo_bronze_files"
ROWS_TABLE = "openmeteo_bronze_rows"
MANIFEST_DIR = Path("/Volumes/Extreme/Mission/manifests/run")

# Expected weather measurement fields
WEATHER_FIELDS = [
    "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
    "cloud_cover_mean",
    "relative_humidity_2m_max", "relative_humidity_2m_min", "relative_humidity_2m_mean",
    "wind_speed_10m_max", "wind_speed_10m_min", "wind_speed_10m_mean",
    "wet_bulb_temperature_2m_min", "wet_bulb_temperature_2m_max", "wet_bulb_temperature_2m_mean",
    "wind_direction_10m_dominant",
    "rain_sum", "precipitation_sum"
]

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
        
        -- Geographic identifiers (denormalized from filename)
        country VARCHAR DEFAULT 'IN',
        state VARCHAR,
        district VARCHAR,
        place_name_norm VARCHAR,
        lat DOUBLE,
        lon DOUBLE,
        lat_tile DOUBLE,
        lon_tile DOUBLE,
        
        -- Temporal dimension
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        reported_date DATE NOT NULL,
        
        -- Weather measurements
        temperature_2m_mean DOUBLE,
        temperature_2m_max DOUBLE,
        temperature_2m_min DOUBLE,
        cloud_cover_mean DOUBLE,
        relative_humidity_2m_max DOUBLE,
        relative_humidity_2m_min DOUBLE,
        relative_humidity_2m_mean DOUBLE,
        wind_speed_10m_max DOUBLE,
        wind_speed_10m_min DOUBLE,
        wind_speed_10m_mean DOUBLE,
        wet_bulb_temperature_2m_min DOUBLE,
        wet_bulb_temperature_2m_max DOUBLE,
        wet_bulb_temperature_2m_mean DOUBLE,
        wind_direction_10m_dominant DOUBLE,
        rain_sum DOUBLE,
        precipitation_sum DOUBLE,
        
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
        f"CREATE INDEX IF NOT EXISTS idx_rows_reported_date ON {ROWS_TABLE}(reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_place_date ON {ROWS_TABLE}(place_name_norm, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_geo_date ON {ROWS_TABLE}(lat_tile, lon_tile, reported_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_year_month ON {ROWS_TABLE}(year, month);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_ingest_job ON {ROWS_TABLE}(ingest_job_id);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_record_hash ON {ROWS_TABLE}(record_hash);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_latest ON {ROWS_TABLE}(is_latest) WHERE is_latest = TRUE;",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_rows_unique_location_date_version ON {ROWS_TABLE}(lat, lon, date, version);",
        f"CREATE INDEX IF NOT EXISTS idx_rows_state_district_date ON {ROWS_TABLE}(state, district, date);",
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
def compute_row_id(file_id: str, date: str, lat: float, lon: float) -> str:
    """Generate deterministic row_id from file_id, date, and coordinates."""
    key = f"{file_id}|{date}|{lat}|{lon}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def compute_record_hash(weather_data: Dict[str, Any]) -> str:
    """Generate hash of weather measurements only (excludes metadata)."""
    # Extract only weather fields in consistent order
    measurements = {k: weather_data.get(k) for k in sorted(WEATHER_FIELDS)}
    key = json.dumps(measurements, sort_keys=True)
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def check_data_quality(record: Dict[str, Any]) -> Tuple[bool, bool, int]:
    """
    Check data quality of a weather record.
    
    Returns:
        (has_nulls, is_complete, null_field_count)
    """
    null_count = 0
    has_nulls = False
    is_complete = True
    
    for field in WEATHER_FIELDS:
        if field not in record:
            is_complete = False
            null_count += 1
        elif record[field] is None:
            has_nulls = True
            null_count += 1
    
    return has_nulls, is_complete, null_count


def parse_date(date_str: str) -> Tuple[str, int, int, int]:
    """
    Parse ISO date string into components.
    
    Returns:
        (date, year, month, day)
    """
    # Handle both 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS.mmm'
    date_part = date_str.split('T')[0]
    dt = datetime.strptime(date_part, '%Y-%m-%d')
    return date_part, dt.year, dt.month, dt.day


def parse_json_payload(payload: str, data_format: str) -> List[Dict[str, Any]]:
    """
    Parse raw_payload into list of records.
    
    Args:
        payload: Raw JSON/JSONL string
        data_format: 'json' or 'jsonl'
    
    Returns:
        List of weather records
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
    lat: float,
    lon: float,
    date: str
) -> List[Dict[str, Any]]:
    """
    Get all existing versions for a specific location and date.
    
    Returns:
        List of dicts with row_id, version, record_hash, is_latest
    """
    query = f"""
    SELECT row_id, version, record_hash, is_latest
    FROM {ROWS_TABLE}
    WHERE lat = ? AND lon = ? AND date = ?
    ORDER BY version DESC
    """
    
    try:
        result = con.execute(query, [lat, lon, date]).fetchall()
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
    
    # Extract metadata from file record
    country = file_record['country']
    state = file_record['state']
    district = file_record['district']
    place_name_norm = file_record['place_name_norm']
    lat = file_record['lat']
    lon = file_record['lon']
    lat_tile = file_record['lat_tile']
    lon_tile = file_record['lon_tile']
    
    # Prepare insert SQL
    insert_sql = f"""
    INSERT INTO {ROWS_TABLE} (
        row_id, file_id, country, state, district, place_name_norm,
        lat, lon, lat_tile, lon_tile,
        reported_date, year, month, reported_day,
        temperature_2m_mean, temperature_2m_max, temperature_2m_min,
        cloud_cover_mean,
        relative_humidity_2m_max, relative_humidity_2m_min, relative_humidity_2m_mean,
        wind_speed_10m_max, wind_speed_10m_min, wind_speed_10m_mean,
        wet_bulb_temperature_2m_min, wet_bulb_temperature_2m_max, wet_bulb_temperature_2m_mean,
        wind_direction_10m_dominant, rain_sum, precipitation_sum,
        has_nulls, is_complete, null_field_count,
        record_hash, is_latest, version,
        ingest_job_id, ingest_ts, source_row_number,
        created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
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
            # Validate date field
            if 'date' not in record:
                logger.warning(f"Record {idx} missing 'date' field in file {file_id}")
                continue
            
            # Parse date
            date_str, year, month, day = parse_date(record['date'])
            
            # Generate IDs and hashes
            row_id = compute_row_id(file_id, date_str, lat, lon)
            record_hash = compute_record_hash(record)
            
            # Check data quality
            has_nulls, is_complete, null_count = check_data_quality(record)
            
            # Check for existing versions
            existing = get_existing_row_versions(con, lat, lon, date_str)
            
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
                row_id, file_id, country, state, district, place_name_norm,
                lat, lon, lat_tile, lon_tile,
                date_str, year, month, day,
                record.get('temperature_2m_mean'),
                record.get('temperature_2m_max'),
                record.get('temperature_2m_min'),
                record.get('cloud_cover_mean'),
                record.get('relative_humidity_2m_max'),
                record.get('relative_humidity_2m_min'),
                record.get('relative_humidity_2m_mean'),
                record.get('wind_speed_10m_max'),
                record.get('wind_speed_10m_min'),
                record.get('wind_speed_10m_mean'),
                record.get('wet_bulb_temperature_2m_min'),
                record.get('wet_bulb_temperature_2m_max'),
                record.get('wet_bulb_temperature_2m_mean'),
                record.get('wind_direction_10m_dominant'),
                record.get('rain_sum'),
                record.get('precipitation_sum'),
                has_nulls, is_complete, null_count,
                record_hash, True, version,
                run_id, now, idx + 1,
                now, now
            ])
            
            rows_inserted += 1
            
            if is_new_version:
                logger.debug(f"Created version {version} for {date_str} at ({lat}, {lon})")
            
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
    run_id = f"parse_meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
    # Strategy: Parse files that haven't been parsed yet or need reprocessing
    query = f"""
    SELECT 
        file_id, checksum, original_filename, file_path,
        country, state, district, place_name_norm,
        lat, lon, lat_tile, lon_tile,
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
            'state': file_row[5],
            'district': file_row[6],
            'place_name_norm': file_row[7],
            'lat': file_row[8],
            'lon': file_row[9],
            'lat_tile': file_row[10],
            'lon_tile': file_row[11],
            'year': file_row[12],
            'month': file_row[13],
            'raw_payload': file_row[14],
            'row_count': file_row[15],
            'data_format': file_row[16]
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