import requests
import time
from datetime import datetime, timedelta
from pathlib import Path


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    # Keep a default Cookie here. Individual calls may override it by passing `cookie`.
    "Cookie": "hd_user_https=3a62edab1d1fdfac16840d966894d901ec697fc36d288396327de293462ad961:1755577834234; hd_user_id_https=180e204db56e9c741902fec14925bfd0a968c5b4b7d33924c32462911fcefc8bb749974b36507219b09ce498b029fb20ac1b28f8dd6081bd0e7c4c60be9a7f52; hd_device_https=166ff9792127320e1097235772cdbe95ec119b7d9c37a6c1fhd_user_https=b8463eee344cbe61e45eb60b84a6adefe821095a2ec026e7e1874629d0b21aef:1761098051944; hd_user_id_https=adf403812ba89d9a3ed7a9f0cf1adf54c678b92753f8188f18d584e80eec41cc86f406009068dc3d229a85d4ce212734e2f766302a7e98a34959bd3960536a82; hd_device_https=8a063e3a3108b287acb76111e554b5544b42aad2c54353e2c095f3e304260f03:bf62bee708056870d92bd7b231933587115e1edc0663e3c0799d8b47b03fda6f; __hd_fingerprint=1847498051833; _gid=GA1.2.259970603.1761098063; _ga_9RGBXZFS3X=GS2.1.s1761098063$o1$g0$t1761098063$j60$l0$h0; _ga=GA1.1.1657724508.1761098060; XSRF-TOKEN=eyJpdiI6Imh5ZDBDdmovS20wQy82UlAxeUlPZmc9PSIsInZhbHVlIjoiYkh4MlJFRlVhdlRnN0NhNk1YYnhNd0R3a0dCbkdiZTdVSWNRT1lpdk9yQlVZelNDeEwrSkRlY1hNbDVJVktsZGhYUk9oeGV5bmxtbkkwVkRNaFd6NFI1WDc3OUVXdGpveE5ZOExUVXVvZWxVT2lrcFFwTTdmVnZmRnJqWG0xT0wiLCJtYWMiOiJhMzU0NDM3YjY0YzUzNjIyN2QyYjQzNzcyYmI5YWJjODMwNTIyMWI2ZDliNWIxOWRlMzY0MzczYmE3YjNmZjFmIiwidGFnIjoiIn0%3D; ncdex_session=eyJpdiI6IjMwUDdSSTZMN1lzZnRlTlpZY3JBWlE9PSIsInZhbHVlIjoiMTVWbTN2NWNrSUF4a0I5OWJKbDF3L0t3SkRGNzBiUTZwWDhndEt6RG9oWXcrSVRIRithWkhxTDJUUTlTbGtSaDBJbzZOSEpGQ3Rrc3gyMWpjZmtJMUwxaEQvUzlzd0FCTXA3bHVFR2RxLy9DejFIQms0TllqazY2UzdvZm1xaUQiLCJtYWMiOiIzODM1Y2ZhNTgwNWE3YzFjOWE2YzBjODk1NmRhZDA4MGI3NjRjMjc5MGUwMmNjZmIwMjg0Y2JiMjllNDI1NDljIiwidGFnIjoiIn0%3D; _ga_6BC6S1TGL7=GS2.1.s1761098059$o1$g1$t1761098224$j24$l0$h0",
    "Host": "ncdex.com",
    "Referer": "https://ncdex.com/markets/bhavcopy",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}

_session = requests.Session()
THROTTLE_SECONDS = 2

def get_bhavcopy(date_str: str, save_path: str, cookie: str = None, timeout: int = 30):
    """Download bhavcopy for a single date.

    Args:
        date_str: date in format "DD-Mon-YYYY" (e.g. "01-Aug-2025")
        save_path: base path to save file
        cookie: optional cookie string to use for this request. If None, the default
            Cookie from the global `headers` will be used.
    """
    # Parse input to standard YYYY-MM-DD
    dt = datetime.strptime(date_str, "%d-%b-%Y")
    save_name = dt.strftime("%Y-%m-%d") + ".zip"
    year = dt.strftime("%Y")
    month = dt.strftime("%b")
    
    url = (
        "https://ncdex.com/markets/bhavcopy?"
        f"file_type_both=final&type_both=bhavcopy&bhavcopy_for=both&filedate_both={date_str}&format_both=file"
    )
    
    # Prepare headers for this request. Copy global headers and optionally override Cookie.
    local_headers = headers.copy()
    if cookie:
        local_headers["Cookie"] = cookie

    folder = Path(save_path) / year / month
    folder.mkdir(parents=True, exist_ok=True)
    full_path = folder / save_name
    
    print(f"Downloading bhavcopy for {date_str} -> {save_name}")
        
    try:
        with _session.get(url, headers=local_headers, stream=True, timeout=timeout) as r:
            if r.status_code != 200:
                print(f"Error {r.status_code} for {date_str}: {r.reason}")
            try:
                print(r.text[:500])
            except Exception:
                pass
            r.raise_for_status()


            with open(full_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"Saved {full_path}")
    
    except requests.exceptions.RequestException as e:
        print(f"Request error for {date_str}: {e}")
    except Exception as e:
        print(f"Unexpected error for {date_str}: {e}")


def get_bhavcopy_range(start_date: str, end_date: str, save_path: str, cookie: str = None):
    """
    Iterate over dates between start_date and end_date (inclusive),
    and call get_bhavcopy for each. You can optionally pass a `cookie` which will be
    used for every request in the range; if cookie is None the default header cookie
    will be used.
    
    Args:
        start_date (str): "YYYY-MM-DD"
        end_date   (str): "YYYY-MM-DD"
        save_path  (str): Base path to save
        cookie (str): Optional cookie string to use for requests
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    while current <= end:
        date_str = current.strftime("%d-%b-%Y")  # e.g. 01-Aug-2025
        get_bhavcopy(date_str, save_path, cookie=cookie)
        time.sleep(THROTTLE_SECONDS)
    current += timedelta(days=1)

if __name__ == "__main__":
    # Example usages:
    # 1) Use default cookie embedded in headers:
    # get_bhavcopy_range("2025-08-01", "2025-10-22", "../data/ncdex")

    # 2) Provide a fresh cookie string for this run (overrides default):
    new_cookie = "_gid=GA1.2.259970603.1761098063; hd_user_https=c8852d319c83724285e92fba54c60d1a3bd74063c57493d4ce49b692f6926883:1761101811544; hd_user_id_https=09e1b4652c9f2197027d4a49d9013e48d53e8c1f9abfc09c269c45914a7f1e87e65560dc0b9cac0d2c1cfd30aeb217e43c009870cec9660d65a4fbfc0703b1f7; hd_device_https=454a1edd34bd77111b1f0a28714077e0f7000920c64a2556802086f3dc2c0842:1fe3b6517d503b4c141733bd679f237f2cacd0674db5681fdd1aa0234c640016; __hd_fingerprint=1847501811636; XSRF-TOKEN=eyJpdiI6IkgrdDYwMnJEUC9GSXVHd2l5OVZsdGc9PSIsInZhbHVlIjoiWTdFZnpXb1dtcnZ6a0I5SnhGQXlCem80YUJpbHI5SitNQmpFa3NneUpVZjQ4SmFramxTbkZOLzRyejlkbldVenY0WTNnSVNLemdDSTFqRHFYdzhTT1ROUy9rTEgwVUJsZHdkVmtJNzBLYm9adlJ2QXlRSWdBSEZ0Y25BL094eXAiLCJtYWMiOiI4Yzk5MmM4ZDljZDgyMzhhMDA2OWJhOTk0NzEyNTgwYTRiYjliNmNiOGRhYmZlZjVlMGM5MzM5OTQ2NGJjNzk2IiwidGFnIjoiIn0%3D; ncdex_session=eyJpdiI6ImhyY1NlSDBaSnBjM1JhWjlIdUZwSmc9PSIsInZhbHVlIjoiOC81QmtmeUx0Wk8zSVFLdm9NZGNRTUlzZ2grYTJkZWJrcldtdDVLZW44M1M2dVd2NXJjT0RTN0kxY29Sbm1sb3Y4YnZCNldYQ2l4dHJkZ3diNUFONGhraG1ZaHdDTVFTQ0Zpa3llaytkb3V1c2J6M0pvUE1tSytRZjI1V3Z3dS8iLCJtYWMiOiIwNGZjNzliMzkwZDFhY2Q0Y2Y3YjZlYjdhNWQ5YzdhZGM1M2MyNDA0MDk4Mjg0NmRmYzE2ZjFiNDk3ZTZhODYyIiwidGFnIjoiIn0%3D; _ga_9RGBXZFS3X=GS2.1.s1761101812$o2$g1$t1761101831$j41$l0$h0; _ga=GA1.1.1657724508.1761098060; _ga_6BC6S1TGL7=GS2.1.s1761101812$o2$g1$t1761102185$j47$l0$h0"
    get_bhavcopy_range("2025-08-01", "2025-10-22", "../data/ncdex", cookie=new_cookie)

