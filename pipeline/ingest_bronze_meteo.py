#!/usr/bin/env python3
"""
pipeline/ingest_bronze_meteo.py

Ingest Open-Meteo JSON / JSONL files into DuckDB table `openmeteo_bronze_files`.
- Parses filenames like: in_andhra_pradesh_anantapur_anantapur_14.678_77.607_1010f03d.json
- Extracts country, state, district, place, lat/lon (when present)
- Computes checksum and row_count (streaming for .jsonl)
- Normalizes place_name for partitioning (place_name_norm)
- Computes lat_tile/lon_tile (rounded to 3 decimals)
- Skips files already ingested (by checksum)
- Emits a small run manifest JSON per run under manifests/run_manifests/
"""

import os
import re
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
import duckdb

# CONFIG
DATA_DIR = Path("data/meteo")          # top-level folder with meteo JSON files
DB_PATH = Path("duckdb/cropai.duckdb")
TABLE = "openmeteo_bronze_files"
MANIFEST_DIR = Path("manifests/run_manifests")
LAT_LON_PRECISION = 3                  # round lat/lon to this many decimals for tiles
IMPORTED_CHECK_COLUMN = "checksum"     # column to check for idempotency

# small canonical list of Indian states for filename parsing (original spacing kept)
STATE_DICT = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa", "Gujarat",
    "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh",
    "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Jammu and Kashmir", "Ladakh", "Delhi", "Puducherry"
]

# Build a normalized -> canonical mapping (underscored, lowercase) to detect multi-word states in filenames
STATE_NORMALIZED = { re.sub(r'\s+', '_', s).lower(): s for s in STATE_DICT }

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Helpers
float_re = re.compile(r"^-?\d+\.\d+$")
float_find_re = re.compile(r"-?\d+\.\d+")

def normalize_place_name(name: str) -> str:
    """Normalize place name into a safe partition key (lowercase, underscores, ascii-ish)."""
    if name is None:
        return None
    n = name.strip().lower()
    # replace spaces and punctuation with underscore
    n = re.sub(r"[^\w]+", "_", n)
    # collapse multiple underscores
    n = re.sub(r"_+", "_", n)
    return n.strip("_")

def file_checksum(path: Path) -> str:
    """
    Compute a SHA256 checksum that includes the file's full path (resolved)
    and the file bytes. This makes the checksum unique to the file *and*
    its location, so repeated files in different folders are distinct.
    """
    h = hashlib.sha256()
    # include resolved absolute path (stable) in hash
    try:
        full_path = str(path.resolve())
    except Exception:
        full_path = str(path)
    h.update(full_path.encode("utf-8"))
    # separator so path and bytes are distinct
    h.update(b"|")
    # then include file bytes
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def count_jsonl_rows(path: Path) -> int:
    """Count lines in a jsonl file streaming (memory-light)."""
    count = 0
    with path.open('rb') as f:
        for _ in f:
            count += 1
    return count

def extract_lat_lon_from_parts(parts):
    """Find candidate float tokens in filename parts and return first pair (lat, lon) if present."""
    floats = []
    for p in parts:
        # allow tokens like 14.678 or 14.678_77.607 etc.
        for m in float_find_re.findall(p):
            floats.append(m)
    if len(floats) >= 2:
        try:
            lat = float(floats[0])
            lon = float(floats[1])
            return lat, lon
        except Exception:
            return None, None
    return None, None

def find_state_district_place(parts):
    """
    Attempt to parse state (multi-word), district, place from filename parts (underscore separated).
    Strategy: scan parts and try to match normalized state token (which may be multi-token).
    Returns (state, district, place) - raw strings or None.
    """
    n = len(parts)
    for i in range(1, n):  # skip country token at parts[0]
        # try multi-token matches up to 4 tokens (state names won't be that long)
        for take in (3, 2, 1):
            if i + take - 1 >= n:
                continue
            candidate = "_".join(parts[i:i+take]).lower()
            if candidate in STATE_NORMALIZED:
                state = STATE_NORMALIZED[candidate]
                # next tokens are district and place if available
                district = parts[i+take] if i+take < n else None
                place = parts[i+take+1] if i+take+1 < n else None
                # clean None -> None
                return state, district, place
    # fallback: if no state parsed, attempt to use second token as district/place
    district = parts[1] if len(parts) > 1 else None
    place = parts[2] if len(parts) > 2 else None
    return None, district, place

def extract_year_month_from_path(path: Path):
    """
    Extract (year, month_number) from a file path like:
      '.../2025 Apr/in_andhra_pradesh_...json'
      '.../2025-04/in_andhra_pradesh_...'
      '.../2025/Apr/...'
    Falls back to file's modified time if not found.
    """
    MONTH_MAP = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12
    }

    year = None
    month = None

    for part in path.parts:
        # 1. direct year folder (e.g., '2025')
        if part.isdigit() and len(part) == 4:
            year = int(part)
        # 2. combined '2025-04' or '2025-Apr'
        elif re.match(r"^\d{4}[-_]", part):
            yr_part, mon_part = re.split(r"[-_]", part, 1)
            if yr_part.isdigit():
                year = int(yr_part)
            mon_key = mon_part.lower()[:3]
            month = MONTH_MAP.get(mon_key)
        # 3. separate month folder ('Apr', 'April', '04')
        elif part.lower()[:3] in MONTH_MAP:
            month = MONTH_MAP[part.lower()[:3]]
        elif part.isdigit() and len(part) == 2:
            # folder like "04"
            month = int(part)

    # fallback to file modification time if missing
    if year is None or month is None:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        year = year or mtime.year
        month = month or mtime.month

    return year, month


# Main
def main():
    con = duckdb.connect(str(DB_PATH))
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"bronze_meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    manifest = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "ingested_files": [],
        "skipped_files": [],
        "errors": []
    }

    # prepare parameterized insert (DuckDB supports ? placeholders)
    insert_sql = f"""
    INSERT INTO {TABLE} (
        file_id, original_filename, country, state, district, place_name_raw,
        place_name_norm, lat, lon, lat_tile, lon_tile, geo_type,
        year, month, checksum, raw_payload, row_count, partition_path,
        ingest_job_id, ingest_ts, created_at
    ) VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ,?,?
    )
    """

    # Query existing checksums to avoid re-ingest (small table, okay to pull)
    try:
        rows = con.execute(f"SELECT checksum FROM {TABLE}").fetchall()
        existing_checksums = set(r[0] for r in rows if r[0] is not None)
    except Exception:
        existing_checksums = set()

    for root, dirs, files in os.walk(DATA_DIR):
        for fname in sorted(files):
            if not fname.endswith('.json'):
                continue
            fpath = Path(root) / fname
            try:
                # parse filename tokens (strip extension)
                base = fname.rsplit('.', 1)[0]
                parts = base.split('_')
                country = parts[0] if len(parts) > 0 else None

                # find state/district/place using robust logic
                state, district, place = find_state_district_place(parts)

                # attempt to extract lat/lon tokens from filename parts
                lat, lon = extract_lat_lon_from_parts(parts)

                # normalized place name for partitioning
                place_norm = normalize_place_name(place or district or base)

                # compute lat/lon tiles (rounded)
                lat_tile = round(lat, LAT_LON_PRECISION) if lat is not None else None
                lon_tile = round(lon, LAT_LON_PRECISION) if lon is not None else None

                # year/month
                year, month = extract_year_month_from_path(fpath)

                checksum = file_checksum(fpath)
                if checksum in existing_checksums:
                    logging.info("Skipping (already ingested): %s", fpath)
                    manifest["skipped_files"].append(str(fpath))
                    continue

                # read raw payload
                raw_payload = None
                if fname.endswith('.json'):
                    # don't load entire file into memory for counting; we will read text for raw_payload but it's fine for small class files
                    # .json - try to detect if it's a list
                    with fpath.open('r', encoding='utf-8') as fh:
                        text = fh.read()
                    try:
                        j = json.loads(text)
                        if isinstance(j, list):
                            row_count = len(j)
                        else:
                            row_count = 1
                    except Exception:
                        # non-JSON or malformed -> mark as 1 and store raw text
                        row_count = 1
                    raw_payload = text

                # partition path suggested
                partition_path = None
                if place_norm:
                    partition_path = f"place_name_norm={place_norm}/year={year}/month={month}"
                elif lat_tile is not None and lon_tile is not None:
                    partition_path = f"lat_tile={lat_tile}/lon_tile={lon_tile}/year={year}/month={month}"
                else:
                    partition_path = f"unknown_place/year={year}/month={month}"

                file_id = checksum  # use checksum as canonical file_id for idempotency
                ingest_job_id = run_id
                now = datetime.now()

                # Insert into DuckDB
                con.execute(insert_sql, [
                    file_id,
                    fname,
                    country,
                    state,
                    district,
                    place,
                    place_norm,
                    lat,
                    lon,
                    lat_tile,
                    lon_tile,
                    ("place_name" if place_norm else "latlon" if lat is not None else "unknown"),
                    int(year) if year is not None else None,
                    int(month) if month is not None else None,
                    checksum,
                    raw_payload,
                    int(row_count),
                    partition_path,
                    ingest_job_id,
                    now,
                    now
                ])

                existing_checksums.add(checksum)
                manifest["ingested_files"].append({
                    "path": str(fpath),
                    "file_id": file_id,
                    "rows": row_count,
                    "partition_path": partition_path
                })
                logging.info("Ingested: %s rows=%s -> %s", fpath, row_count, TABLE)

            except Exception as e:
                logging.exception("Failed to ingest %s", fpath)
                manifest["errors"].append({
                    "path": str(fpath),
                    "error": str(e)
                })

    # finalize manifest
    manifest["ended_at"] = datetime.now().isoformat()
    manifest_path = MANIFEST_DIR / f"manifest_{run_id}.json"
    with manifest_path.open("w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2, default=str)
    logging.info("Run complete. Manifest written to %s", manifest_path)

    con.close()


if __name__ == "__main__":
    main()
