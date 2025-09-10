# -*- coding: utf-8 -*-
"""
grab_iran_shops_allcities.py

کارکرد: برای لیستی از شهرها (پیش‌فرض: از فایل iran_cities.txt یا لیستِ داخلی)
 - برای هر شهر: bbox را از Nominatim می‌گیرد، آن را به کاشی‌ها تقسیم می‌کند،
   هر کاشی را از Overpass می‌گیرد (با mirror fallback و retries)،
   نتایج را تجمیع و dedupe می‌کند و برای هر شهر یک فایل CSV به نام "<City>.csv" می‌سازد.
 - فقط CSV خروجی می‌دهد (UTF-8 with BOM / utf-8-sig).
 - اگر فایل "<City>.csv" وجود داشته باشد، به‌صورت پیش‌فرض آن شهر را نادیده می‌گیرد (resume).
 - پیکربندی‌ها در بالای فایل قابل تنظیم‌اند.
"""
import requests, time, json, os, re
from urllib.parse import quote_plus
from collections import defaultdict
import pandas as pd
from tqdm import tqdm

# ----------------- تنظیمات -----------------
USER_AGENT = "ShopFinderAllCities/1.0 (contact: youremail@example.com)"  # حتما ایمیل خودت را اینجا بگذار
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter"
]

# اگر می‌خواهی همه‌شهرها را از فایل بخواند: یک نام فایل قرار بده؛
# فرمت هر خط: Tehran یا تهران (هر دو کار می‌کنند)
CITIES_FILE = "iran_cities.txt"   # اگر وجود نداشت از DEFAULT_CITIES استفاده می‌شود

# لیستی از شهرهای پیش‌فرض (چند شهر مهم؛ می‌تونی این لیست را بزرگ‌تر کنی)
DEFAULT_PROVINCES = [
    "Tehran",
    "Alborz",
    "Qom",
    "Qazvin",
    "Markazi",
    "Gilan",
    "Mazandaran",
    "Golestan",
    "Semnan",
    "Khorasan Razavi",
    "North Khorasan",
    "South Khorasan",
    "Esfahan",
    "Yazd",
    "Kerman",
    "Hormozgan",
    "Sistan and Baluchestan",
    "Fars",
    "Bushehr",
    "Kohgiluyeh and Boyer-Ahmad",
    "Chaharmahal and Bakhtiari",
    "Khuzestan",
    "Lorestan",
    "Ilam",
    "Kermanshah",
    "Hamedan",
    "Zanjan",
    "Ardabil",
    "East Azerbaijan",
    "West Azerbaijan",
    "Kurdistan"
]



# تقسیم کاشی برای هر شهر (هرچه عدد بزرگ‌تر، کاشی‌ها کوچکتر و جزئی‌تر؛ زمان اجرا بیشتر)
TILE_NX = 6
TILE_NY = 6

# محدودیت‌ها / تأخیرها و retry
TIMEOUT_PER_REQUEST = 90
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
SLEEP_BETWEEN_TILES = 1.0
SLEEP_BETWEEN_CITIES = 5.0

# مسیر خروجی csv
OUT_DIR = "output_csvs"
os.makedirs(OUT_DIR, exist_ok=True)

# اگر بخواهی مجدداً روی شهری که خروجی دارد دوباره کار کند:
FORCE_REWRITE = False

# -------------------------------------------

def sanitize_filename(name):
    name = name.strip()
    # حذف کاراکترهای نامناسب برای فایل‌سیستم
    name = re.sub(r'[\\/:"*?<>|]+', '_', name)
    return name

def load_cities(file_path=CITIES_FILE):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            cities = [line.strip() for line in f if line.strip()]
            if cities:
                return cities
    return DEFAULT_CITIES.copy()

def nominatim_bbox(city_name, email=None, timeout=10):
    params = {"q": city_name, "format": "json", "limit": 1}
    headers = {"User-Agent": USER_AGENT}
    if email:
        params["email"] = email
    r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if not j:
        return None
    bb = j[0]["boundingbox"]  # [south, north, west, east]
    south = float(bb[0]); north = float(bb[1]); west = float(bb[2]); east = float(bb[3])
    display_name = j[0].get("display_name","")
    return (south, west, north, east, display_name)

def split_bbox(bbox, nx, ny):
    south, west, north, east = bbox
    lat_step = (north - south) / ny
    lon_step = (east - west) / nx
    boxes = []
    for i in range(ny):
        for j in range(nx):
            s = south + i * lat_step
            n = south + (i+1) * lat_step
            w = west + j * lon_step
            e = west + (j+1) * lon_step
            boxes.append((s, w, n, e))
    return boxes

def build_overpass_template():
    parts = [
        'node["shop"]({bbox});',
        'way["shop"]({bbox});',
        'relation["shop"]({bbox});',
        'node["amenity"="restaurant"]({bbox});',
        'way["amenity"="restaurant"]({bbox});',
        'node["amenity"="cafe"]({bbox});',
        'node["amenity"="fast_food"]({bbox});',
    ]
    return "\n".join(parts)

def query_overpass_with_mirrors(query_body, timeout=TIMEOUT_PER_REQUEST, max_retries=MAX_RETRIES):
    headers = {"User-Agent": USER_AGENT}
    last_exc = None
    for mirror in OVERPASS_MIRRORS:
        for attempt in range(1, max_retries+1):
            try:
                full_q = f"[out:json][timeout:{timeout}];({query_body});out center tags;"
                r = requests.post(mirror, data={"data": full_q}, headers=headers, timeout=timeout)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as he:
                status = getattr(he.response, "status_code", None)
                print(f"    -> Mirror {mirror} HTTP {status}, attempt {attempt}/{max_retries}")
                last_exc = he
            except requests.exceptions.RequestException as e:
                print(f"    -> Mirror {mirror} request error {type(e).__name__}, attempt {attempt}/{max_retries}")
                last_exc = e
            sleep_for = BACKOFF_BASE ** attempt
            time.sleep(sleep_for)
        print(f"    !! Mirror {mirror} exhausted, trying next mirror.")
    raise RuntimeError("All Overpass mirrors failed") from last_exc

def dedupe_elements(elems):
    seen = set()
    out = []
    for el in elems:
        key = (el.get("type"), el.get("id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(el)
    return out

def extract_shop_record(el, city_name):
    tags = el.get("tags", {}) or {}
    name = tags.get("name","")
    phone = tags.get("contact:phone") or tags.get("phone") or tags.get("telephone") or ""
    email = tags.get("contact:email") or tags.get("email") or ""
    website = tags.get("website") or tags.get("contact:website") or ""
    addr_parts = []
    for k in ("addr:street","addr:housenumber","addr:city","addr:postcode","addr:state"):
        v = tags.get(k)
        if v:
            addr_parts.append(v)
    address = ", ".join(addr_parts) if addr_parts else (tags.get("addr:full") or "")
    return {
        "city": city_name,
        "osm_type": el.get("type"),
        "osm_id": el.get("id"),
        "name": name,
        "address": address,
        "phone": phone,
        "email": email,
        "website": website
    }

def process_city(city_name):
    safe_name = sanitize_filename(city_name)
    out_path = os.path.join(OUT_DIR, f"{safe_name}.csv")
    if os.path.exists(out_path) and not FORCE_REWRITE:
        print(f"[skip] {city_name} -> {out_path} exists (skip, set FORCE_REWRITE=True to overwrite)")
        return

    print(f"\n[city] Starting: {city_name}")
    try:
        s,w,n,e,display = nominatim_bbox(city_name, email=None)
        bbox = (s,w,n,e)
        print(f"  bbox: {bbox}  ({display})")
    except Exception as ex:
        print("  ! Nominatim failed:", ex)
        return

    tiles = split_bbox(bbox, TILE_NX, TILE_NY)
    template = build_overpass_template()
    all_elements = []
    failed_tiles = []

    for idx, tb in enumerate(tiles):
        s,w,n,e = tb
        bbox_str = f"{s},{w},{n},{e}"
        query_body = template.format(bbox=bbox_str)
        print(f"  tile {idx+1}/{len(tiles)} bbox={tb} ...", end="", flush=True)
        try:
            data = query_overpass_with_mirrors(query_body)
            elems = data.get("elements", [])
            print(f" {len(elems)} elems")
            all_elements.extend(elems)
        except Exception as ex:
            print(" FAILED:", type(ex).__name__, str(ex)[:200])
            failed_tiles.append({"idx": idx, "bbox": tb, "error": str(ex)})
        time.sleep(SLEEP_BETWEEN_TILES)

    all_elements = dedupe_elements(all_elements)
    print(f"  -> total unique elements: {len(all_elements)} (failed tiles: {len(failed_tiles)})")

    records = [extract_shop_record(el, city_name) for el in all_elements]
    if not records:
        print(f"  ! No records for {city_name}. Saved debug file for inspection.")
        debug_path = os.path.join(OUT_DIR, f"{safe_name}_raw.json")
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump({"elements": all_elements, "failed_tiles": failed_tiles}, f, ensure_ascii=False, indent=2)
        return

    df = pd.DataFrame(records)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  => saved {len(df)} rows to {out_path}")

def main():
    cities = load_cities()
    print(f"Cities to process: {len(cities)}")
    for i, city in enumerate(cities, start=1):
        print(f"\n[{i}/{len(cities)}] Processing city: {city}")
        try:
            process_city(city)
        except Exception as e:
            print("  !! unexpected error processing city:", type(e).__name__, e)
        print(f"Sleeping {SLEEP_BETWEEN_CITIES}s before next city...")
        time.sleep(SLEEP_BETWEEN_CITIES)
    print("\nAll done. CSV files in:", os.path.abspath(OUT_DIR))

if __name__ == "__main__":
    main()
