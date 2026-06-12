"""
  - get_ad_ids(page) để gom list_id
  - crawl_detail(id)  để lấy raw JSON chi tiết
Lưu 2 file:
  data/all_ids.json        — set các list_id đã biết
  data/all_raw_data.json   — { list_id: raw_ad_json }
"""
import json
import time
from pathlib import Path

import requests

HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)

ID_FILE = DATA_DIR / "all_ids.json"
RAW_FILE = DATA_DIR / "all_raw_data.json"

REGION_V2 = 12000   # Hà Nội
CATEGORY = 1000     # Bất động sản
PAGE_START = 0
PAGE_END = 3        
LIMIT = 20
SLEEP_LIST = 1.5
SLEEP_DETAIL = 0.8

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Apple Silicon)",
    "Accept": "application/json",
}


def load_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def get_ad_ids(page: int) -> list:
    offset = page * LIMIT
    url = (
        "https://gateway.chotot.com/v1/public/ad-listing"
        f"?region_v2={REGION_V2}&cg={CATEGORY}&o={offset}&limit={LIMIT}"
    )
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return [ad["list_id"] for ad in r.json().get("ads", []) if "list_id" in ad]


def crawl_detail(ad_id) -> dict:
    url = f"https://gateway.chotot.com/v1/public/ad-listing/{ad_id}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()


def step_collect_ids() -> set:
    all_ids = set(load_json(ID_FILE, []))
    print(f"[IDs] load sẵn: {len(all_ids)}")
    for page in range(PAGE_START, PAGE_END):
        try:
            ids = get_ad_ids(page)
        except Exception as e:
            print(f"[IDs] page {page + 1} lỗi: {e}")
            time.sleep(SLEEP_LIST * 2)
            continue
        before = len(all_ids)
        all_ids.update(ids)
        print(f"[IDs] page {page + 1}: +{len(all_ids) - before} (tổng {len(all_ids)})")
        time.sleep(SLEEP_LIST)
    save_json(ID_FILE, list(all_ids))
    return all_ids


def step_crawl_details(all_ids: set) -> dict:
    raw = load_json(RAW_FILE, {})
    crawled = set(raw.keys())
    print(f"[RAW] đã có {len(crawled)} records")
    todo = [str(x) for x in all_ids if str(x) not in crawled]
    print(f"[RAW] cần crawl thêm: {len(todo)}")
    for i, ad_id in enumerate(todo, 1):
        try:
            data = crawl_detail(ad_id)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"[RAW] 404 bỏ qua {ad_id}")
                all_ids.discard(int(ad_id)) if ad_id.isdigit() else all_ids.discard(ad_id)
                continue
            print(f"[RAW] HTTP error {ad_id}: {e}")
            continue
        except Exception as e:
            print(f"[RAW] err {ad_id}: {e}")
            continue
        raw[ad_id] = data
        if i % 20 == 0:
            save_json(RAW_FILE, raw)
            print(f"[RAW] checkpoint {i}/{len(todo)} (tổng lưu {len(raw)})")
        time.sleep(SLEEP_DETAIL)
    save_json(RAW_FILE, raw)
    save_json(ID_FILE, list(all_ids))
    print(f"[RAW] hoàn tất: {len(raw)} records vào {RAW_FILE}")
    return raw


def main() -> None:
    print("=" * 60)
    print("CRAWL NHATOT/CHOTOT")
    print("=" * 60)
    all_ids = step_collect_ids()
    step_crawl_details(all_ids)


if __name__ == "__main__":
    main()
