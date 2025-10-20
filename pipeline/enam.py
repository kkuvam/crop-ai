import os
import json
import time
import requests
from pathlib import Path
import gzip, json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Iterable


headers = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

def get_states():
    url = "https://enam.gov.in/web/ajax_ctrl/states_name"
    response = requests.get(url)
    return response.json()


def get_apmcs():
    apmcs = []
    url = "https://enam.gov.in/web/Ajax_ctrl/apmc_list"
    states = get_states()['data']
    # Extract all state_ids
    state_ids = [item['state_id'] for item in states]
    for state_id in state_ids:
        payload = { 'state_id': state_id}
        response = requests.post(url, data=payload, headers=headers)
        apmcs.extend(response.json()['data'])
    return apmcs


def date_range(start_date: str, end_date: str):
    """
    Yields date strings (YYYY-MM-DD) from start_date to end_date inclusive.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def save_jsonl_gz(data: list, jsonl_path: str) -> None:
    """
    Append DataFrame rows to a JSONL file (one JSON object per line).
    """
    if not data:
        return
    with gzip.open(jsonl_path, "wt", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def to_date(s: str) -> datetime:
    """
    Parse a date string into a datetime object.
    Supports common formats like:
    - YYYY-MM-DD
    - DD-MM-YYYY
    - DD/MM/YYYY
    - YYYY/MM/DD
    - MM/DD/YYYY
    """
    try:
        return datetime.fromisoformat(s)  # handles YYYY-MM-DD
    except ValueError:
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
    raise ValueError(f"Unrecognized date format: {s}")


def get_trade_data(date_str: str) -> list:
    url = "https://enam.gov.in/web/Ajax_ctrl/trade_data_list"
    payload = {
        "language": "en",
        "stateName": "-- All --",
        "apmcName": "-- Select APMCs --",
        "commodityName": "-- Select Commodity --"    
    }
    payload["fromDate"] = date_str
    payload["toDate"] = date_str
    response = requests.post(url, data=payload, headers=headers)
    response = response.json()
    if "data" in response:
        return response["data"]
    else:
        print("No 'data' field found in response:", response)
        return None


def get_trade_data_range(start_date: str, end_date: str, jsonl_path: str) -> list:
    all_data = []
    for d in date_range(start_date, end_date):
        dt = datetime.strptime(d, "%Y-%m-%d")
        date_str = dt.strftime("%Y-%m-%d")
        year_str = dt.strftime("%Y") 
        month_str = dt.strftime("%b") 
        data = get_trade_data(date_str)
        day_dir = Path(jsonl_path)
        out_path = day_dir / year_str / month_str / f"{date_str}.jsonl.gz"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Processing for date: {date_str}")
        save_jsonl_gz(data, out_path)
        if data:  # checks if data exists and is not empty/None
            all_data.extend(data)
    return all_data


if __name__ == "__main__":
    output_location = "../data/enam/"
    start_date = "2011-01-01"
    end_date = "2019-12-31"
    trade_data_list = get_trade_data_range(start_date, end_date, output_location)
