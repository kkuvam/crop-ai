import requests
from datetime import datetime, timedelta
from pathlib import Path


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Cookie": "hd_user_https=3a62edab1d1fdfac16840d966894d901ec697fc36d288396327de293462ad961:1755577834234; hd_user_id_https=180e204db56e9c741902fec14925bfd0a968c5b4b7d33924c32462911fcefc8bb749974b36507219b09ce498b029fb20ac1b28f8dd6081bd0e7c4c60be9a7f52; hd_device_https=166ff9792127320e1097235772cdbe95ec119b7d9c37a6c1f0e2fb799993427c:bae811cf7a3b9d898de12cdebfd883830763064939506dfd0b1b8d48f07c0a0a; __hd_fingerprint=1841977834249; _gid=GA1.2.1203954946.1755577836; XSRF-TOKEN=eyJpdiI6IjNKRlVKV3J5MUtBSC9tNmxEaGppSVE9PSIsInZhbHVlIjoiaGFsWmgzd05FNkhHK1dpVFhpMmQ0K1liRHBQNnJkQTBEeFdvSlJlK3dxeS9GRFhROEdTUFA1RkVMREk0Vy9NcTBsWFFKRnJCTnRWQUhab0pHRFpsY0Y3dE1tZ29QbnRXUi9HNEkrQVRGZEFyTEswZUNRc0VEVWZINldIT3dRSGEiLCJtYWMiOiJiYTNiZjExNmJiN2I0MzlkYjY4NGIyMTIwODNiYTBhMjZhNzYwYTU4ZjZlYzE0NGYwZTFjZjg1NGE0OTBkZGI2IiwidGFnIjoiIn0%3D; ncdex_session=eyJpdiI6IkM2dXkybVYwcTFBR1l6a1A1NnNnQ3c9PSIsInZhbHVlIjoiNXM5VXEyL29GZWhHTFlHMm5kOE5BRUs1TTFNaXNSQkNxUE5sa1pYUHZnUzJnYVdTU1VIUy9EL0JReFVhRWJQSjM5OVUxdmFnN2hhL25XNDFYR1lEbGNMNi9ZNldCa0NGNUlnbWoya3Yza3Fic0RaN3pCTUd4djRxVWpjSnk3SVciLCJtYWMiOiJkMjliY2RlMDFlMGUwYzNjNWIyOTkyNmFjYjBiZjQyMmQ0MDM5Y2NmM2Q0ZjJlMzE0MWYxMzQ5ZjRmMmU5ODc1IiwidGFnIjoiIn0%3D; _ga_9RGBXZFS3X=GS2.1.s1755625514$o2$g1$t1755625624$j59$l0$h0; _ga=GA1.2.939189129.1755577835; _gat_gtag_UA_13122686_1=1; _ga_6BC6S1TGL7=GS2.1.s1755625514$o2$g1$t1755625639$j44$l0$h0",
    "Host": "ncdex.com",
    "Referer": "https://ncdex.com/markets/bhavcopy",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}


def get_bhavcopy(date_str: str, save_path: str):
    # Parse input to standard YYYY-MM-DD
    dt = datetime.strptime(date_str, "%d-%b-%Y")
    save_name = dt.strftime("%Y-%m-%d") + ".zip"
    year = dt.strftime("%Y")
    month = dt.strftime("%b")
    
    url = (
        "https://ncdex.com/markets/bhavcopy?"
        f"file_type_both=final&type_both=bhavcopy&bhavcopy_for=both&filedate_both={date_str}&format_both=file"
    )
    
    try:
        folder = Path(save_path) / year / month
        folder.mkdir(parents=True, exist_ok=True)
        full_path = folder / save_name
        
        # Send a GET request to the URL. The 'stream=True' parameter
        # is helpful for large files as it avoids loading the entire
        # file into memory at once.
        print(f"Downloading bhavcopy for {date_str} -> {save_name}")
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(full_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive
                        f.write(chunk)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def get_bhavcopy_range(start_date: str, end_date: str, save_path: str):
    """
    Iterate over dates between start_date and end_date (inclusive),
    and call download_bhavcopy for each.
    
    Args:
        start_date (str): "YYYY-MM-DD"
        end_date   (str): "YYYY-MM-DD"
        save_path  (str): Base path to save
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    while current <= end:
        date_str = current.strftime("%d-%b-%Y")  # e.g. 01-Aug-2025
        try:
            get_bhavcopy(date_str, save_path)
        except Exception as e:
            print(f"❌ Failed for {date_str}: {e}")
        current += timedelta(days=1)

if __name__ == "__main__":
    get_bhavcopy_range("2024-01-01", "2024-12-31", "../data/ncdex")