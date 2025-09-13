from datetime import datetime, timedelta
from urllib.parse import urlencode, urlparse, parse_qs
from typing import List, Optional, Dict
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import gzip, json
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def get_columns():
    return [
        "state_name",
        "district_name",
        "market_name",
        "variety",
        "group",
        "arrivals",
        "min_price",
        "max_price",
        "modal_price",
        "reported_date",
        "grade"
    ]


def get_commodities():
    url = "https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_State=0&Tx_District=0&Tx_Market=0&Tx_Trend=2&DateFrom=17-Aug-2025&DateTo=17-Aug-2025&Fr_Date=17-Aug-2025&To_Date=17-Aug-2025&Tx_CommodityHead=Ajwan&Tx_Commodity=137"
    response = requests.get(url)    
    soup = BeautifulSoup(response.content, "html.parser") 
    select = soup.find("select", {"id": "ddlCommodity"})
    commodities = []
    if select:
        for option in select.find_all("option"):
            value = option.get("value")
            text = option.text.strip()
            if value != "0":
                commodities.append({"value": value, "text": text})
    return commodities


def build_url(commodity: dict, start_date: str, end_date:str) -> str:
    uri = "https://agmarknet.gov.in/SearchCmmMkt.aspx"
    
    # Convert YYYY-MM-DD -> DD-Mon-YYYY (e.g., 2025-08-17 -> 17-Aug-2025)
    def format_date(date_str: str) -> str:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    start_fmt = format_date(start_date)
    end_fmt   = format_date(end_date)

    params = {
        "Tx_State": 0,
        "Tx_District": 0,
        "Tx_Market": 0,
        "Tx_Trend": 2,
        "DateFrom": start_fmt,
        "DateTo": end_fmt,
        "Fr_Date": start_fmt,
        "To_Date": end_fmt,
        "Tx_CommodityHead": commodity["text"],
        "Tx_Commodity": commodity["value"],
    }
    return f"{uri}?{urlencode(params)}"


def commodity_from_url(url: str) -> str | None:
    qs = parse_qs(urlparse(url).query, keep_blank_values=True)
    val = qs.get("Tx_CommodityHead", [None])[0]
    return val.strip() if isinstance(val, str) else None


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


def save_jsonl_gz(df: pd.DataFrame, jsonl_path: str) -> None:
    """
    Append DataFrame rows to a JSONL file (one JSON object per line).
    """
    if df is None or df.empty:
        return
    with gzip.open(jsonl_path, "at", encoding="utf-8") as f:
        df.to_json(f, orient="records", lines=True, force_ascii=False)


def read_range(dates, base_dir="data/commodities") -> pd.DataFrame:
    """
    Read multiple date-partitioned JSONL.GZ files into a single DataFrame.
    dates: list of 'YYYY-MM-DD' strings
    base_dir/YYYY-MM-DD.jsonl.gz is expected layout.
    """
    frames = []
    for d in dates:
        paths = glob.glob(f"{base_dir}/{d}.jsonl.gz")
        for path in paths:
            # Stream read JSONL into DataFrame
            df = pd.read_json(path, lines=True, compression="gzip")
            frames.append(df)

    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def scrape_table_to_df( driver, url: str, css_selector: str = "table.tableagmark_new", wait_seconds: int = 2,
) -> pd.DataFrame:
    """
    Navigate to `url`, wait for table with `css_selector`, parse to DataFrame, rename columns,
    optionally append to JSONL, and return the DataFrame. If multiple tables match, returns the first.
    """
    driver.get(url)

    # Wait for the table to be present in DOM
    WebDriverWait(driver, wait_seconds).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
    )

    tables = driver.find_elements(By.CSS_SELECTOR, "table.tableagmark_new")
    table_html = tables[0].get_attribute("outerHTML")
    df = pd.read_html(StringIO(table_html))[0]
    df.columns = get_columns()
    df["commodity_name"] = commodity_from_url(url)
    df = df[df["group"] != "Total:-"].reset_index(drop=True)
    return df


def iterate_commodity_and_scrape(
    start_date: str,
    end_date: str,
    RECYCLE_EVERY = 100,
    wait_seconds = 2
) -> pd.DataFrame:
    """
    Iterate from start_date to end_date (inclusive), call `scrape_table_to_df` for each date,
    and return a concatenated DataFrame.

    Parameters:
      - driver
      - start_date/end_date: strings parsable by datetime.fromisoformat or strptime; use YYYY-MM-DD recommended
      - date_format_in_url: how {date} should be formatted inside the URL (default "dd-mm-YYYY")
    """
    
    
    d0 = to_date(start_date)
    d1 = to_date(end_date)

    all_dfs: List[pd.DataFrame] = []
    driver = make_driver()
    count = 0

    current = d0
    while current <= d1:
        date_str = current.strftime("%Y-%m-%d")
        year_str = current.strftime("%Y") 
        month_str = current.strftime("%b") 
        # Per-day output path: base_dir/date=YYYY-MM-DD/part-000.jsonl.gz
        day_dir = Path(base_dir)
        out_path = out_path = day_dir / year_str / month_str / f"{date_str}.jsonl.gz"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        commodities = get_commodities()
        for commodity in commodities:
            url = build_url(commodity, date_str, date_str)
            try:
                # Recycle proactively
                if count > 0 and count % RECYCLE_EVERY == 0:
                    driver.quit()
                    driver = make_driver()

                print(f"Pulling data from: {url}")
                df = scrape_table_to_df(driver, url)
                print("Pulled data: ", df.shape)
                save_jsonl_gz(df, out_path)
                count += 1
            except Exception as e:
                print(f"[WARN] {date_str}: No data could be retrieved for commodity {commodity}")
                # print(e)
                driver.quit()
                driver = make_driver()


        current += timedelta(days=1)

    driver.quit()
    return True


def iterate_date_and_scrape(
    start_date: str,
    end_date: str,
    base_dir: str,
    RECYCLE_EVERY = 100,
    per_request_sleep: float = 0.5
) -> pd.DataFrame:
    """
    For each day in [start, end], call iterate_commodity_and_scrape(driver, day, day),
    append results to base_dir/YYYY-MM-DD.jsonl.gz, and return the combined DataFrame.

    Args:
        driver: Selenium WebDriver
        start_date, end_date: date strings (accepts YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY, etc.)
        base_dir: base directory where date partitions will be written
        per_request_sleep: seconds to sleep between days (politeness/backoff)
        overwrite: if True, removes existing per-day output before writing

    Returns:
        pd.DataFrame with all rows across the range (empty if none)
    """
    d0 = to_date(start_date).date()
    d1 = to_date(end_date).date()

    all_frames = []
    current = d0
    while current <= d1:
        day_iso = current.strftime("%Y-%m-%d")
        
        try:
            
            iterate_commodity_and_scrape(day_iso, day_iso)
        except Exception as e:
            print(f"[WARN] {day_iso}: {e}")

        if per_request_sleep > 0:
            time.sleep(per_request_sleep)

        current += timedelta(days=1)

    return True


def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")              # esp. in Linux containers
    opts.add_argument("--disable-dev-shm-usage")   # esp. in Linux containers
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })
    driver = webdriver.Chrome(options=opts)  # Selenium Manager auto-resolves driver
    driver.set_page_load_timeout(5)
    driver.set_script_timeout(5)
    return driver


if __name__ == "__main__":
    base_dir ="../data/agmarknet"
    start = "2024-01-01"
    end = "2024-12-31"
    RECYCLE_EVERY = 100

    iterate_date_and_scrape(start, end, base_dir, RECYCLE_EVERY)

