#!/usr/bin/env python3
"""
pipeline/ingest_bronze_meteo.py

Ingest Open-Meteo JSON / JSONL files into DuckDB table `openmeteo_bronze_files`.
- The data in duckdb is partitioned by place_name 
- Parses filenames like: in_andhra_pradesh_anantapur_anantapur_14.678_77.607_1010f03d.json
- Extracts country, state, district, place, lat/lon (when present)
- Computes checksum and row_count (streaming for .jsonl)
- Normalizes place_name for partitioning (place_name_norm)
- Computes lat_tile/lon_tile (rounded to 3 decimals)
- Skips files already ingested (by checksum)
- Emits a small run manifest JSON per run under manifests/run_manifests/

IMPROVED SCHEMA:
----------------
CREATE TABLE IF NOT EXISTS openmeteo_bronze_files (
    -- Primary identification
    file_id VARCHAR PRIMARY KEY,                    -- checksum-based unique ID
    checksum VARCHAR NOT NULL UNIQUE,               -- SHA256 checksum for deduplication
    original_filename VARCHAR NOT NULL,             -- original file name
    file_path VARCHAR NOT NULL,                     -- full resolved path
    file_size_bytes BIGINT,                         -- file size for monitoring
    
    -- Geographic metadata (parsed from filename)
    country VARCHAR DEFAULT 'IN',                   -- e.g., 'IN' for India
    state VARCHAR,                                  -- e.g., 'Andhra Pradesh'
    district VARCHAR,                               -- e.g., 'Anantapur'
    place_name_raw VARCHAR,                         -- original place name
    place_name_norm VARCHAR,                        -- normalized (for partitioning)
    
    -- Coordinates
    lat DOUBLE,                                     -- latitude from filename
    lon DOUBLE,                                     -- longitude from filename
    lat_tile DOUBLE,                                -- rounded latitude (for tiling)
    lon_tile DOUBLE,                                -- rounded longitude (for tiling)
    geo_type VARCHAR,                               -- 'place_name', 'latlon', 'unknown'
    
    -- Temporal metadata
    year INTEGER,                                   -- data year
    month INTEGER,                                  -- data month (1-12)
    
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
CREATE INDEX IF NOT EXISTS idx_checksum ON openmeteo_bronze_files(checksum);
CREATE INDEX IF NOT EXISTS idx_place_norm ON openmeteo_bronze_files(place_name_norm);
CREATE INDEX IF NOT EXISTS idx_year_month ON openmeteo_bronze_files(year, month);
CREATE INDEX IF NOT EXISTS idx_geo_tiles ON openmeteo_bronze_files(lat_tile, lon_tile);
CREATE INDEX IF NOT EXISTS idx_ingest_job ON openmeteo_bronze_files(ingest_job_id);
CREATE INDEX IF NOT EXISTS idx_created_at ON openmeteo_bronze_files(created_at);
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


# ============================================================================
# CONFIGURATION
# ============================================================================
DATA_DIR = Path("/Volumes/Extreme/Mission/data/meteo")
DB_PATH = Path("/Volumes/Extreme/Mission/duckdb/cropai.duckdb")
TABLE = "openmeteo_bronze_files"
MANIFEST_DIR = Path("/Volumes/Extreme/Mission/manifests/run")

LAT_LON_PRECISION = 3           # round lat/lon to this many decimals for tiles
CHUNK_SIZE = 65536              # 64KB chunks for file reading
MAX_PAYLOAD_SIZE = 100_000_000  # 100MB max payload (safety limit)

# State dictionary for parsing Indian states
STATE_DICT = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", 
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", 
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", 
    "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", 
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal", 
    "Jammu and Kashmir", "Ladakh", "Delhi", "Puducherry"
]

STATE_NORMALIZED = {re.sub(r'\s+', '_', s).lower(): s for s in STATE_DICT}

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
        -- Primary identification
        file_id VARCHAR PRIMARY KEY,
        checksum VARCHAR NOT NULL UNIQUE,
        original_filename VARCHAR NOT NULL,
        file_path VARCHAR NOT NULL,
        file_size_bytes BIGINT,
        
        -- Geographic metadata
        country VARCHAR DEFAULT 'IN',
        state VARCHAR,
        district VARCHAR,
        place_name_raw VARCHAR,
        place_name_norm VARCHAR,
        
        -- Coordinates
        lat DOUBLE,
        lon DOUBLE,
        lat_tile DOUBLE,
        lon_tile DOUBLE,
        geo_type VARCHAR,
        
        -- Temporal metadata
        year INTEGER,
        month INTEGER,
        
        -- Content metadata
        raw_payload VARCHAR,
        row_count INTEGER DEFAULT 1,
        data_format VARCHAR,
        
        -- Partitioning
        partition_path VARCHAR,
        
        -- Ingestion tracking
        ingest_job_id VARCHAR NOT NULL,
        ingest_ts TIMESTAMP NOT NULL,
        ingest_duration_ms INTEGER,
        
        -- Audit timestamps
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for performance
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_checksum ON {TABLE}(checksum);",
        f"CREATE INDEX IF NOT EXISTS idx_place_norm ON {TABLE}(place_name_norm);",
        f"CREATE INDEX IF NOT EXISTS idx_year_month ON {TABLE}(year, month);",
        f"CREATE INDEX IF NOT EXISTS idx_geo_tiles ON {TABLE}(lat_tile, lon_tile);",
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
def normalize_place_name(name: Optional[str]) -> Optional[str]:
    """Normalize place name into a safe partition key (lowercase, underscores)."""
    if not name:
        return None
    n = name.strip().lower()
    n = re.sub(r"[^\w]+", "_", n)
    n = re.sub(r"_+", "_", n)
    return n.strip("_")


def file_checksum(
    path: Path,
    include_path: bool = True,
    chunk_size: int = 65536,
    algorithm: str = "sha256"
) -> str:
    """
    Compute a cryptographic checksum of a file.
    
    Args:
        path: Path to the file to checksum
        include_path: If True, includes the resolved absolute path in the hash,
                     making the checksum unique to both content and location.
                     If False, only file contents are hashed.
        chunk_size: Size of chunks to read (default 64KB for better I/O performance)
        algorithm: Hash algorithm to use (default: sha256)
    
    Returns:
        Hexadecimal string representation of the checksum
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        PermissionError: If the file can't be read
        IsADirectoryError: If the path points to a directory
        
    Example:
        >>> checksum = file_checksum(Path("data.txt"))
        >>> # Check if file was already processed
        >>> if checksum in processed_files:
        ...     print("File already processed")
    """
    # Validate path exists and is a file
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    if path.is_dir():
        raise IsADirectoryError(f"Path is a directory, not a file: {path}")
    
    # Initialize hash object
    try:
        h = hashlib.new(algorithm)
    except ValueError:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    # Include resolved absolute path if requested
    if include_path:
        try:
            full_path = str(path.resolve())
        except (OSError, RuntimeError) as e:
            # Fallback to absolute path if resolve fails (e.g., symlink issues)
            full_path = str(path.absolute())
        
        h.update(full_path.encode("utf-8"))
        # Use null byte as separator (more standard than pipe)
        h.update(b"\x00")
    
    # Hash file contents in chunks
    try:
        with path.open("rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
    except PermissionError:
        raise PermissionError(f"Permission denied reading file: {path}")
    except Exception as e:
        raise IOError(f"Error reading file {path}: {e}")
    
    return h.hexdigest()


def file_checksum_with_metadata(
    path: Path,
    include_size: bool = True,
    include_mtime: bool = False
) -> str:
    """
    Compute a fast checksum including file metadata instead of full content.
    Useful for quick duplicate detection when file size/mtime are sufficient.
    
    Args:
        path: Path to the file
        include_size: Include file size in hash
        include_mtime: Include modification time in hash
        
    Returns:
        Hexadecimal string representation of the checksum
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    h = hashlib.sha256()
    
    # Include path
    h.update(str(path.resolve()).encode("utf-8"))
    h.update(b"\x00")
    
    # Include file size
    if include_size:
        stat = path.stat()
        h.update(str(stat.st_size).encode("utf-8"))
        h.update(b"\x00")
    
    # Include modification time
    if include_mtime:
        stat = path.stat()
        h.update(str(stat.st_mtime_ns).encode("utf-8"))
        h.update(b"\x00")
    
    return h.hexdigest()


def count_jsonl_rows(path: Path) -> int:
    """Count lines in JSONL file (streaming, memory-efficient)."""
    count = 0
    with path.open('rb') as f:
        for _ in f:
            count += 1
    return count


def extract_lat_lon_from_parts(parts: list) -> Tuple[Optional[float], Optional[float]]:
    """Extract first two float values from filename parts as (lat, lon)."""
    floats = []
    for p in parts:
        for m in FLOAT_FIND_RE.findall(p):
            try:
                floats.append(float(m))
            except ValueError:
                continue
    
    if len(floats) >= 2:
        return floats[0], floats[1]
    return None, None


def find_state_district_place(parts: list) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse state (multi-word), district, place from filename parts.
    Returns: (state, district, place) - canonical names or None.
    """
    n = len(parts)
    
    # Try to match multi-word state names first
    for i in range(1, n):  # skip country token at parts[0]
        for take in (3, 2, 1):  # try 3-word, 2-word, 1-word matches
            if i + take - 1 >= n:
                continue
            
            candidate = "_".join(parts[i:i+take]).lower()
            if candidate in STATE_NORMALIZED:
                state = STATE_NORMALIZED[candidate]
                district = parts[i+take] if i+take < n else None
                place = parts[i+take+1] if i+take+1 < n else None
                return state, district, place
    
    # Fallback: no state found
    district = parts[1] if len(parts) > 1 else None
    place = parts[2] if len(parts) > 2 else None
    return None, district, place


def extract_year_month_from_path(path: Path) -> Tuple[int, int]:
    """
    Extract (year, month) from file path or fallback to file mtime.
    Handles formats: '2025 Apr', '2025-04', '2025/Apr/', etc.
    """
    MONTH_MAP = {
        "jan": 1, "january": 1, "feb": 2, "february": 2,
        "mar": 3, "march": 3, "apr": 4, "april": 4,
        "may": 5, "jun": 6, "june": 6,
        "jul": 7, "july": 7, "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11,
        "dec": 12, "december": 12
    }
    
    year = None
    month = None
    
    for part in path.parts:
        # Direct year folder (e.g., '2025')
        if part.isdigit() and len(part) == 4:
            year = int(part)
        
        # Combined format '2025-04' or '2025-Apr'
        elif re.match(r"^\d{4}[-_]", part):
            yr_part, mon_part = re.split(r"[-_]", part, 1)
            if yr_part.isdigit():
                year = int(yr_part)
            mon_key = mon_part.lower()[:3]
            month = MONTH_MAP.get(mon_key)
        
        # Separate month folder ('Apr', 'April', '04')
        elif part.lower()[:3] in MONTH_MAP:
            month = MONTH_MAP[part.lower()[:3]]
        elif part.isdigit() and len(part) == 2:
            month = int(part)
    
    # Fallback to file modification time
    if year is None or month is None:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        year = year or mtime.year
        month = month or mtime.month
    
    return year, month


def build_partition_path(
    place_norm: Optional[str],
    lat_tile: Optional[float],
    lon_tile: Optional[float],
    year: int,
    month: int
) -> str:
    """Build suggested partition path based on available metadata."""
    if place_norm:
        return f"place_name_norm={place_norm}/year={year}/month={month:02d}"
    elif lat_tile is not None and lon_tile is not None:
        return f"lat_tile={lat_tile}/lon_tile={lon_tile}/year={year}/month={month:02d}"
    else:
        return f"unknown_place/year={year}/month={month:02d}"


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
    file_size = fpath.stat().st_size
    data_format = 'jsonl' if fname.endswith('.jsonl') else 'json'
    
    # Parse filename components
    country = parts[0] if len(parts) > 0 else None
    state, district, place = find_state_district_place(parts)
    lat, lon = extract_lat_lon_from_parts(parts)
    place_norm = normalize_place_name(place or district or base)
    
    # Compute tiles
    lat_tile = round(lat, LAT_LON_PRECISION) if lat is not None else None
    lon_tile = round(lon, LAT_LON_PRECISION) if lon is not None else None
    
    # Temporal
    year, month = extract_year_month_from_path(fpath)
    
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
    
    # Partition path
    partition_path = build_partition_path(place_norm, lat_tile, lon_tile, year, month)
    
    # Geo type
    if place_norm:
        geo_type = "place_name"
    elif lat is not None:
        geo_type = "latlon"
    else:
        geo_type = "unknown"
    
    duration_ms = int((time.time() - start_time) * 1000)
    
    return {
        'file_id': checksum,
        'checksum': checksum,
        'original_filename': fname,
        'file_path': str(fpath.resolve()),
        'file_size_bytes': file_size,
        'country': country,
        'state': state,
        'district': district,
        'place_name_raw': place,
        'place_name_norm': place_norm,
        'lat': lat,
        'lon': lon,
        'lat_tile': lat_tile,
        'lon_tile': lon_tile,
        'geo_type': geo_type,
        'year': year,
        'month': month,
        'raw_payload': raw_payload,
        'row_count': row_count,
        'data_format': data_format,
        'partition_path': partition_path,
        'ingest_duration_ms': duration_ms
    }


def get_existing_checksums(con: duckdb.DuckDBPyConnection) -> Set[str]:
    """Fetch all existing checksums from database for deduplication."""
    try:
        rows = con.execute(f"SELECT checksum FROM {TABLE}").fetchall()
        return {r[0] for r in rows if r[0] is not None}
    except Exception as e:
        logger.warning(f"Could not fetch existing checksums: {e}")
        return set()


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
    run_id = f"bronze_meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
        country, state, district, place_name_raw, place_name_norm,
        lat, lon, lat_tile, lon_tile, geo_type,
        year, month, raw_payload, row_count, data_format,
        partition_path, ingest_job_id, ingest_ts, ingest_duration_ms,
        created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?
    )
    """
    
    # Get existing checksums for deduplication
    existing_checksums = get_existing_checksums(con)
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
                    metadata['state'],
                    metadata['district'],
                    metadata['place_name_raw'],
                    metadata['place_name_norm'],
                    metadata['lat'],
                    metadata['lon'],
                    metadata['lat_tile'],
                    metadata['lon_tile'],
                    metadata['geo_type'],
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