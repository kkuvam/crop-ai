#!/usr/bin/env python3
"""
pipeline/parse_bronze_meteo.py

Read raw JSON payloads from `openmeteo_bronze_files.raw_payload` and
explode them into `openmeteo_bronze_rows`.

Assumptions:
- `openmeteo_bronze_files` exists and contains rows inserted by
  ingest_bronze_meteo.py with columns including:
    file_id, original_filename, place_name_norm, lat, lon, lat_tile, lon_tile,
    year, month, checksum, raw_payload, parse_status, ingest_job_id, ingest_ts
- Target table `openmeteo_bronze_rows` exists (DDL created earlier).

Idempotency:
- We compute a row-level checksum and skip inserting rows whose checksum already exists.

What the script does:
- For each candidate file in openmeteo_bronze_files:
  - parse raw_payload (JSON array or JSONL)
  - for each daily record produce a row with canonical columns and raw_vars JSON string
  - compute row_checksum and skip if present
  - insert rows into openmeteo_bronze_rows
  - update bronze file's parse_status to PARSED (or PARSED_WITH_WARNINGS on partial failures)
- write a run manifest to manifests/run_manifests/
"""

import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, Any

import duckdb

# CONFIG
DB_PATH = Path("duckdb/cropai.duckdb")
MANIFEST_DIR = Path("manifests/run_manifests")
BRONZE_FILES_TABLE = "openmeteo_bronze_files"
BRONZE_ROWS_TABLE = "openmeteo_bronze_rows"
RUN_PREFIX = "parse_bronze_meteo"
LAT_LON_PRECISION = 3

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def safe_iso_to_datetime(s: str) -> datetime:
    """Parse ISO-ish datetime strings robustly (handles fractions and optional Z)."""
    if s is None:
        return None
    s = s.strip()
    # Remove trailing Z if present (naive)
    if s.endswith("Z"):
        s = s[:-1]
    # Python's fromisoformat accepts "YYYY-MM-DDTHH:MM:SS.mmm"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # fallback common patterns
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
    # as last resort, try splitting date part
    try:
        return datetime.strptime(s.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def compute_row_checksum(file_id: str, date_str: str, raw_vars_obj: Dict[str, Any]) -> str:
    """Compute deterministic SHA256 checksum for a row using file_id + date + sorted JSON."""
    h = hashlib.sha256()
    h.update((file_id or "").encode("utf-8"))
    h.update(b"|")
    h.update((date_str or "").encode("utf-8"))
    h.update(b"|")
    # Stable serialization of raw_vars
    try:
        stable = json.dumps(raw_vars_obj, sort_keys=True, ensure_ascii=False)
    except Exception:
        stable = str(raw_vars_obj)
    h.update(stable.encode("utf-8"))
    return h.hexdigest()


def normalize_place_name(name: str) -> str:
    if name is None:
        return None
    n = name.strip().lower()
    n = "".join(ch if ch.isalnum() else "_" for ch in n)
    n = "_".join(part for part in n.split("_") if part)
    return n or None


def round_tile(value):
    return round(value, LAT_LON_PRECISION) if value is not None else None


def load_existing_row_checksums(con) -> set:
    """Load existing row_checksum values to skip duplicates (may be large; acceptable for class project)."""
    try:
        rows = con.execute(f"SELECT row_checksum FROM {BRONZE_ROWS_TABLE} WHERE row_checksum IS NOT NULL").fetchall()
        return set(r[0] for r in rows)
    except Exception:
        # Table may be empty or missing
        return set()


def parse_payload_text(payload_text: str):
    """
    Payload may be:
      - a JSON array string: [ {...}, {...} ]
      - a JSONL text: newline-separated JSON objects
    Return list of dicts.
    """
    payload_text = payload_text.strip()
    if not payload_text:
        return []
    # Try parse as JSON array
    try:
        parsed = json.loads(payload_text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # sometimes payload is a single dict (wrap into list)
            return [parsed]
    except Exception:
        # fallthrough to JSONL
        pass

    # Parse JSONL: one JSON object per line
    records = []
    for line in payload_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            records.append(obj)
        except Exception:
            # skip malformed line but record later as warning
            logging.warning("Skipping malformed JSONL line during parse.")
    return records


def main():
    con = duckdb.connect(str(DB_PATH))
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"{RUN_PREFIX}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    manifest = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "files_processed": [],
        "files_skipped": [],
        "errors": []
    }

    # Prepare insert SQL for bronze rows
    insert_sql = f"""
    INSERT INTO {BRONZE_ROWS_TABLE} (
        row_id, file_id, place_name_raw, place_name_norm, lat, lon,
        lat_tile, lon_tile, grid_id, date_utc, timestamp_utc,
        raw_vars, ingest_job_id, row_checksum, parse_warnings, partition_hint, original_record_index, created_at
    ) VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
    )
    """

    # fetch candidate files from bronze manifest table
    try:
        bronze_files = con.execute(f"""
            SELECT file_id, original_filename, place_name_norm, lat, lon, lat_tile, lon_tile,
                   year, month, checksum, raw_payload, parse_status, ingest_job_id
            FROM {BRONZE_FILES_TABLE}
            WHERE raw_payload IS NOT NULL
            ORDER BY ingest_ts ASC
        """).fetchall()
    except Exception as e:
        logging.error("Failed to read bronze files table: %s", e)
        con.close()
        return

    existing_checksums = load_existing_row_checksums(con)
    logging.info("Loaded %d existing row checksums", len(existing_checksums))

    for bf in bronze_files:
        try:
            (file_id, original_filename, place_name_norm, lat, lon, lat_tile, lon_tile,
             year, month, checksum, raw_payload, parse_status, ingest_job_id) = bf

            logging.info("Processing bronze file: %s (file_id=%s)", original_filename, file_id)

            records = parse_payload_text(raw_payload or "")
            if not records:
                logging.warning("No records parsed for file %s", original_filename)
                manifest["files_skipped"].append({"file": original_filename, "reason": "no_records"})
                # update parse_status to PARSED_WITH_WARNINGS
                con.execute(f"UPDATE {BRONZE_FILES_TABLE} SET parse_status = ? WHERE file_id = ?", ["PARSED_WITH_WARNINGS", file_id])
                continue

            inserted = 0
            skipped = 0
            warnings = []

            for idx, rec in enumerate(records):
                # DAILY record expected keys; keep raw_vars as original sub-dict
                # date field can be "date" or "time" depending on source; prefer "date"
                date_str = rec.get("date") or rec.get("time") or rec.get("datetime") or rec.get("timestamp")
                dt = safe_iso_to_datetime(date_str) if date_str else None
                date_utc = dt.date() if dt else None
                timestamp_utc = dt if dt else None

                # canonicalize raw_vars: keep only the DAILY_VARS present in rec
                # but store entire rec as raw_vars for traceability
                raw_vars_obj = rec  # keep the whole record; minimal transform

                # compute deterministic checksum
                row_checksum = compute_row_checksum(file_id, str(date_str), raw_vars_obj)

                if row_checksum in existing_checksums:
                    skipped += 1
                    continue

                # build row fields
                place_raw = place_name_norm or None
                place_norm = normalize_place_name(place_raw) if place_raw else None

                row_lat = lat
                row_lon = lon
                row_lat_tile = lat_tile if lat_tile is not None else (round_tile(row_lat) if row_lat is not None else None)
                row_lon_tile = lon_tile if lon_tile is not None else (round_tile(row_lon) if row_lon is not None else None)

                # grid_id not available in payload example; leave None
                grid_id = rec.get("grid_id")

                partition_hint = None
                if place_norm:
                    partition_hint = f"place_name_norm={place_norm}/year={year}/month={month}"
                elif row_lat_tile is not None and row_lon_tile is not None:
                    partition_hint = f"lat_tile={row_lat_tile}/lon_tile={row_lon_tile}/year={year}/month={month}"
                else:
                    partition_hint = f"unknown_place/year={year}/month={month}"

                row_id = row_checksum  # use checksum as unique row id

                now = datetime.now()
                parse_warnings = None

                # Insert row
                try:
                    con.execute(insert_sql, [
                        row_id,
                        file_id,
                        place_raw,
                        place_norm,
                        row_lat,
                        row_lon,
                        row_lat_tile,
                        row_lon_tile,
                        grid_id,
                        date_utc,
                        timestamp_utc,
                        json.dumps(raw_vars_obj, ensure_ascii=False),
                        ingest_job_id,
                        row_checksum,
                        parse_warnings,
                        partition_hint,
                        idx,
                        now
                    ])
                    inserted += 1
                    existing_checksums.add(row_checksum)
                except Exception as ie:
                    logging.exception("Failed to insert row for file %s idx=%d: %s", original_filename, idx, ie)
                    warnings.append({"idx": idx, "error": str(ie)})
                    # continue parsing next rows

            # Update parse_status on bronze_files
            new_status = "PARSED"
            if warnings:
                new_status = "PARSED_WITH_WARNINGS"
            con.execute(f"UPDATE {BRONZE_FILES_TABLE} SET parse_status = ? WHERE file_id = ?", [new_status, file_id])

            manifest["files_processed"].append({
                "file": original_filename,
                "file_id": file_id,
                "inserted": inserted,
                "skipped": skipped,
                "warnings": warnings
            })
            logging.info("File %s: inserted=%d skipped=%d warnings=%d", original_filename, inserted, skipped, len(warnings))

        except Exception as e:
            logging.exception("Error processing bronze file record: %s", e)
            manifest["errors"].append({"file_row": str(bf), "error": str(e)})

    manifest["ended_at"] = datetime.now().isoformat()
    manifest_path = MANIFEST_DIR / f"manifest_{run_id}.json"
    with manifest_path.open("w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2, default=str)
    logging.info("Parse run complete. Manifest written to %s", manifest_path)

    con.close()


if __name__ == "__main__":
    main()
