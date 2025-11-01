"""
pipeline/ingest_bronze_common.py

Common helper functions for bronze ETL scripts (agmarket, enam, meteo).
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

CHUNK_SIZE = 65536
MAX_PAYLOAD_SIZE = 100_000_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

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



def extract_year_month_from_path(path: Path) -> Tuple[int, int]:
    MONTH_MAP = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    year = None
    month = None
    for part in path.parts:
        if part.isdigit() and len(part) == 4:
            year = int(part)
        elif part.lower()[:3] in MONTH_MAP:
            month = MONTH_MAP[part.lower()[:3]]
        elif part.isdigit() and len(part) == 2:
            month = int(part)
    if year is None or month is None:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        year = year or mtime.year
        month = month or mtime.month
    return year, month


def get_existing_checksums(con: duckdb.DuckDBPyConnection, table: str) -> Set[str]:
    """Fetch all existing checksums from database for deduplication."""
    try:
        rows = con.execute(f"SELECT checksum FROM {table}").fetchall()
        return {r[0] for r in rows if r[0] is not None}
    except Exception as e:
        # Use local logger if available, else print
        try:
            logger.warning(f"Could not fetch existing checksums: {e}")
        except NameError:
            print(f"Could not fetch existing checksums: {e}")
        return set()

