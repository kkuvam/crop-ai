import os
import re
import json
import math
import hashlib
import calendar
import time
import argparse
from datetime import datetime, date, timedelta
import pandas as pd
from typing import List, Optional, Literal
from collections import deque
from time import sleep
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Throttle params
MIN_INTERVAL = 4   # ~1 calls/sec ≈ 60 calls/minute
MAX_PER_MINUTE = 30   # your safe cap
SLEEP_SECONDS = 30    # hard sleep when limit is hit

# Initialise
_last_call = 0.0
_recent_calls = deque()

# Variables/order for the data
DAILY_VARS = [
    "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
    "cloud_cover_mean",
    "relative_humidity_2m_max", "relative_humidity_2m_min", "relative_humidity_2m_mean",
    "wind_speed_10m_max", "wind_speed_10m_min", "wind_speed_10m_mean", 
    "wet_bulb_temperature_2m_min", "wet_bulb_temperature_2m_max", "wet_bulb_temperature_2m_mean",
    "wind_direction_10m_dominant",
    "rain_sum", "precipitation_sum"
]

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

def save_jsonl(data: list, file_path: str) -> list:
    """Save a list of dicts to JSONL."""
    with open(file_path, "w", encoding="utf-8") as f:
        for obj in data:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def load_jsonl(file_path: str) -> list:
    """Load a JSONL file and return a list of locations (strings)."""
    records = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():  # skip empty lines
                obj = json.loads(line)
                records.append(obj)
    return records

def get_cache(file_path: str = ".open-meteo") -> set:
    """Load cached keys from file."""
    if not os.path.exists(file_path):
        return set()
    with open(file_path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def set_cache(key: str, file_path: str = ".open-meteo") -> None:
    """Add key to cache file."""
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(key + "\n")

def build_openmeteo_client(cache_path: str = ".cache") -> openmeteo_requests.Client:
    cache_session = requests_cache.CachedSession(cache_path, expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def normalize(text: str) -> str:
    """Helper to normalize names."""
    text = text.lower().strip()
    text = re.sub(r"\s+", "_", text)      # spaces -> underscores
    text = re.sub(r"[^a-z0-9_]", "", text)  # keep only safe chars
    return text

def make_location_filename(data: dict, ext: str = 'json') -> str:
    # Extract and normalize key parts
    country = normalize(data.get("country_code", "in"))
    state = normalize(data.get("state_name", "unknown"))
    district = normalize(data.get("district_name", "unknown"))
    
    # Prefer market_name, fallback to apmc_name
    market_raw = data.get("market_name") or data.get("apmc_name") or "unknown"
    market = normalize(market_raw)
    
    # Lat/long rounded to 3 decimals for disambiguation
    lat = round(float(data.get("latitude", 0.0)), 3)
    lon = round(float(data.get("longitude", 0.0)), 3)

    # Short hash from resolved_name+lat+lon for uniqueness
    unique_str = f"{data.get('resolved_name', '')}{lat}{lon}"
    hash_part = hashlib.sha256(unique_str.encode()).hexdigest()[:8]

    # Build filename
    filename = f"{country}_{state}_{district}_{market}_{lat}_{lon}_{hash_part}.{ext}"
    return filename

def add_suffix(filename: str, suffix: str) -> str:
    base, ext = os.path.splitext(filename)
    return f"{base}_{suffix}{ext}"

def fetch_daily(client: openmeteo_requests.Client,
                url: str,
                lat: float,
                lon: float,
                start_date: date,
                end_date: date,
                timezone: str
                ) -> pd.DataFrame:
    """Fetch daily data from either archive or forecast endpoint."""
    if start_date > end_date:
        return pd.DataFrame(columns=["date", *DAILY_VARS])
    
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
        "daily": DAILY_VARS, "timezone": timezone,
    }
    
    responses = client.weather_api(url, params=params)
    if not responses:
        return pd.DataFrame(columns=["date", *DAILY_VARS])
    daily = responses[0].Daily()

    # Build UTC series -> convert to local tz -> take local calendar date
    t_start_utc = pd.to_datetime(daily.Time(), unit="s", utc=True)
    t_end_utc = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
    interval_s = int(daily.Interval())

    # Create UTC range, then convert to local tz and take just the calendar date
    time_utc = pd.date_range(start=t_start_utc, end=t_end_utc,
                             freq=pd.Timedelta(seconds=interval_s), inclusive="left")
    time_local = time_utc.tz_convert(timezone)
    date_local = time_local.normalize().date  # array of python date objects

    daily_data = {"date": pd.Series(date_local)}

    # Assign in the same order
    for i, key in enumerate(DAILY_VARS):
        daily_data[key] = daily.Variables(i).ValuesAsNumpy()

    df = pd.DataFrame(daily_data)
    
    # Final clamp (defensive)
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].reset_index(drop=True)

    return df

def write_partitioned_by_month(
    df: pd.DataFrame,
    base_dir: str,
    location_meta: dict,
    filename_suffix: Optional[str] = None  # e.g., "forecast"
) -> List[str]:
    """
    Writes df partitioned by month into ./base_dir/YYYY/Mon/<filename[+suffix]>.json
    Returns list of saved paths.
    """
    saved_paths: List[str] = []
    if df.empty:
        return saved_paths

    # Build base filename
    fname = make_location_filename(location_meta, ext="json")
    if filename_suffix:
        fname = add_suffix(fname, filename_suffix)
    
    # Add Year/Month columns to group easily
    df["_Y"] = pd.to_datetime(df["date"]).dt.year
    df["_M"] = pd.to_datetime(df["date"]).dt.month
    
    # Group by year+month (handles multi-year spans too)
    for (yy, mm), g in df.groupby(["_Y", "_M"], sort=True):
        month_df = g.drop(columns=["_Y", "_M"]).reset_index(drop=True)

        year_dir = os.path.join(base_dir, f"{int(yy):04d}")
        month_dir = os.path.join(year_dir, calendar.month_abbr[int(mm)])  # 'Apr'
        os.makedirs(month_dir, exist_ok=True)

        fpath = os.path.join(month_dir, fname)
        month_df.to_json(fpath, orient="records", date_format="iso")
        saved_paths.append(fpath)

    return saved_paths

def save_openmeteo_month(
    location_meta: dict,
    year: int,
    mode: Literal["archive", "forecast"] = "archive",
    base_dir: str = "../data/meteo",
    timezone: str = "Asia/Kolkata",
    client: Optional[openmeteo_requests.Client] = None,
    horizon_days: int = 15,   # only used by forecast & hybrid
) -> str:
    """
    Save month JSON under ../data/meteo/YYYY/Apr/<filename>.json

    mode:
      - 'archive'  : archive only (end clamped to today-5)
      - 'forecast' : forecast only (start=max(month_start, today), end=today+horizon_days)
      - 'hybrid'   : archive (<= D-5) + forecast (>= D-4), saved as '<name>_forecast.json'
    """
    if "latitude" not in location_meta or "longitude" not in location_meta:
        print("location_meta must include 'latitude' and 'longitude'.")
        return []
    
    lat, lon = float(location_meta["latitude"]), float(location_meta["longitude"])
    
    # Compute intended local date window for the year
    start_date_local = date(year, 1, 1)
    end_date_local = date(year, 12, 31)

    # Sanity clamp: do not request beyond "today" (local)
    today_local = datetime.now().date()
    archive_tail = today_local - timedelta(days=5)
    
    if end_date_local > archive_tail:
        end_date_local = archive_tail

    # If the entire year is in the future -> nothing to fetch
    if start_date_local > today_local:
        return []

    # Setup Open-Meteo client with cache + retries
    openmeteo = client or build_openmeteo_client()
    
    if mode == "archive":
        start = start_date_local
        end = end_date_local
        df = fetch_daily(openmeteo, ARCHIVE_URL, lat, lon, start, end, timezone)
        return write_partitioned_by_month(df, base_dir, location_meta)

    elif mode == "forecast":
        start = today_local  # today onwards
        end = today_local + timedelta(days=horizon_days)
        df = fetch_daily(openmeteo, FORECAST_URL, lat, lon, start, end, timezone)
        return write_partitioned_by_month(df, base_dir, location_meta, filename_suffix="forecast")

    else:
        raise ValueError("mode must be one of: 'archive', 'forecast'")

def throttle():
    """If too many calls in last TIME_WINDOW, sleep until next slot."""
    global _last_call, _recent_calls
    now = time.time()
    wait = (_last_call + MIN_INTERVAL) - now
    if wait > 0:
        print(f"[throttle] Sleeping {wait:.2f}s to respect MIN_INTERVAL")
        time.sleep(wait)
    _last_call = time.time()

    while _recent_calls and _recent_calls[0] < now - 60:
        _recent_calls.popleft()

    if len(_recent_calls) >= MAX_PER_MINUTE:
        print(f"[pause] Hit {MAX_PER_MINUTE} calls/min, sleeping {SLEEP_SECONDS}s…")
        time.sleep(SLEEP_SECONDS)
        _recent_calls.clear()
        _recent_calls.append(time.time())
    else:
        _recent_calls.append(now)

def run_archive(year: int):
    """Run archive mode for the specified year."""
    client = build_openmeteo_client()
    locs = load_jsonl("mandies_20250907.jsonl")
    cache = get_cache()
    print(f"Processing {len(locs)} records for archive (year {year})...")
    start_str = time.strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    print(f"Script started at: {start_str}")
    count = 1
    try:
        for loc in locs:
            key = f"{make_location_filename(loc, ext='json')}:{year}:archive"
            if key in cache:
                print(f"Skipping {count} : {loc.get('resolved_name', '')} (cached, key: {key})...")
                count += 1
                continue
            print(f"Processing {count} : {loc.get('resolved_name', '')} ...")
            try:
                if not loc.get("latitude") or not loc.get("longitude"):
                    print(f"Skipping {count} : {loc.get('resolved_name', '')} (no coordinates)")
                    count += 1
                    continue
                out_path = save_openmeteo_month(
                    location_meta=loc,
                    year=year,
                    base_dir="../data/meteo",
                    client=client,
                    mode="archive"
                )
                if out_path:  # Only cache if successfully saved
                    throttle()
                    set_cache(key)
                    cache.add(key)  # Update in-memory cache
            except Exception as e:
                if str(e).find("Hourly API request limit exceeded") != -1:
                    print(f"[rate_limit] Hourly API limit exceeded, sleeping for 3600s...")
                    time.sleep(3600)  # Sleep for 1 hour
                    continue  # Retry the same location
                raise  # Re-raise other exceptions
            count += 1
    except Exception as e:
        fail_str = time.strftime("%Y-%m-%d %H:%M:%S")
        duration = time.time() - start_time
        print(f"Script failed at: {fail_str}")
        print(f"Total run time before failure: {duration:.2f} seconds")
        print(f"Error: {str(e)}")
        raise


def run_forecast():
    """Run forecast mode."""
    client = build_openmeteo_client()
    locs = load_jsonl("mandies_20250907.jsonl")
    cache = get_cache()
    print(f"Processing {len(locs)} records for forecast...")
    start_str = time.strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    print(f"Script started at: {start_str}")
    count = 1
    try:
        for loc in locs:
            key = f"{make_location_filename(loc, ext='json')}:{2025}:forecast"
            if key in cache:
                print(f"Skipping {count} : {loc.get('resolved_name', '')} (cached, key: {key})...")
                count += 1
                continue
            print(f"Processing {count} : {loc.get('resolved_name', '')} ...")
            try:
                if not loc.get("latitude") or not loc.get("longitude"):
                    print(f"Skipping {count} : {loc.get('resolved_name', '')} (no coordinates)")
                    count += 1
                    continue
                out_path = save_openmeteo_month(
                    location_meta=loc,
                    year=2025,
                    base_dir="../data/meteo",
                    client=client,
                    mode="forecast"
                )
                if out_path:  # Only cache if successfully saved
                    throttle()
                    set_cache(key)
                    cache.add(key)  # Update in-memory cache
            except Exception as e:
                if str(e).find("Hourly API request limit exceeded") != -1:
                    print(f"[rate_limit] Hourly API limit exceeded, sleeping for 600s...")
                    time.sleep(600)  # Sleep for 10 minutes
                    continue  # Retry the same location
                raise  # Re-raise other exceptions
            count += 1
    except Exception as e:
        fail_str = time.strftime("%Y-%m-%d %H:%M:%S")
        duration = time.time() - start_time
        print(f"Script failed at: {fail_str}")
        print(f"Total run time before failure: {duration:.2f} seconds")
        print(f"Error: {str(e)}")
        raise
    

def main():
    parser = argparse.ArgumentParser(description="Fetch Open-Meteo weather data for archive or forecast.")
    parser.add_argument("--year", type=int, choices=[2024, 2025], help="Run archive mode for the specified year (2024 or 2025)")
    parser.add_argument("--forecast", action="store_true", help="Run forecast mode")
    args = parser.parse_args()

    if args.year and args.forecast:
        print("Error: Cannot specify both --year and --forecast. Choose one.")
        return
    if not (args.year or args.forecast):
        print("Error: Must specify either --year (2024 or 2025) or --forecast.")
        return

    if args.year:
        run_archive(args.year)
    elif args.forecast:
        run_forecast()

if __name__ == "__main__":
    main()
