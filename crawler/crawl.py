"""Crawler bất động sản có retry, checkpoint nguyên tử và chế độ chạy liên tục."""
import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)
ID_FILE = DATA_DIR / "all_ids.json"
RAW_FILE = DATA_DIR / "all_raw_data.json"

HEADERS = {
    "User-Agent": "BigDataRealEstateResearchCrawler/1.0",
    "Accept": "application/json",
}

logger = logging.getLogger("real_estate_crawler")
METRICS = {}


@dataclass(frozen=True)
class CrawlerConfig:
    regions: tuple[int, ...]
    category: int = 1000
    page_start: int = 0
    page_end: int = 3
    limit: int = 20
    sleep_list: float = 1.5
    sleep_detail: float = 0.8
    request_timeout: float = 15


def configure_metrics(port: int) -> None:
    if port <= 0:
        return
    try:
        from prometheus_client import Counter, Gauge, start_http_server

        METRICS.update(
            discovered=Counter(
                "crawler_ids_discovered_total", "Số listing ID mới tìm thấy"
            ),
            crawled=Counter(
                "crawler_records_saved_total", "Số listing detail đã lưu"
            ),
            errors=Counter(
                "crawler_errors_total", "Số lỗi crawler", ["stage"]
            ),
            known_ids=Gauge("crawler_known_ids", "Tổng listing ID đã biết"),
            stored_records=Gauge(
                "crawler_stored_records", "Tổng listing detail đã lưu"
            ),
        )
        start_http_server(port)
        logger.info("Prometheus metrics: http://localhost:%d/metrics", port)
    except ImportError:
        logger.warning("Thiếu prometheus-client; metrics endpoint bị tắt")


def metric_inc(name: str, amount: int = 1, stage: str | None = None) -> None:
    metric = METRICS.get(name)
    if metric is None:
        return
    if stage is not None:
        metric.labels(stage=stage).inc(amount)
    else:
        metric.inc(amount)


def metric_set(name: str, value: int) -> None:
    metric = METRICS.get(name)
    if metric is not None:
        metric.set(value)


def create_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        logger.error("Không đọc được checkpoint %s: %s", path, error)
        metric_inc("errors", stage="checkpoint_read")
        return default


def save_json(path: Path, obj) -> None:
    """Ghi checkpoint nguyên tử để process dừng giữa chừng không làm hỏng JSON."""
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temp_path, path)


def get_ad_ids(
    session: requests.Session, page: int, region: int, config: CrawlerConfig
) -> list:
    response = session.get(
        "https://gateway.chotot.com/v1/public/ad-listing",
        params={
            "region_v2": region,
            "cg": config.category,
            "o": page * config.limit,
            "limit": config.limit,
        },
        timeout=config.request_timeout,
    )
    response.raise_for_status()
    return [
        ad["list_id"]
        for ad in response.json().get("ads", [])
        if "list_id" in ad
    ]


def crawl_detail(
    session: requests.Session, ad_id, config: CrawlerConfig
) -> dict:
    response = session.get(
        f"https://gateway.chotot.com/v1/public/ad-listing/{ad_id}",
        timeout=config.request_timeout,
    )
    response.raise_for_status()
    return response.json()


def step_collect_ids(session: requests.Session, config: CrawlerConfig) -> set:
    all_ids = set(load_json(ID_FILE, []))
    logger.info("Đã có %d listing ID", len(all_ids))
    for region in config.regions:
        for page in range(config.page_start, config.page_end):
            try:
                ids = get_ad_ids(session, page, region, config)
            except requests.RequestException as error:
                logger.error("Region %s page %s lỗi: %s", region, page + 1, error)
                metric_inc("errors", stage="listing")
                continue
            before = len(all_ids)
            all_ids.update(ids)
            discovered = len(all_ids) - before
            metric_inc("discovered", discovered)
            logger.info(
                "Region %s page %s: +%d (tổng %d)",
                region,
                page + 1,
                discovered,
                len(all_ids),
            )
            time.sleep(config.sleep_list)
    save_json(ID_FILE, sorted(all_ids, key=str))
    metric_set("known_ids", len(all_ids))
    return all_ids


def step_crawl_details(
    session: requests.Session, all_ids: set, config: CrawlerConfig
) -> dict:
    raw = load_json(RAW_FILE, {})
    todo = [str(value) for value in all_ids if str(value) not in raw]
    logger.info("Đã lưu %d records; cần crawl thêm %d", len(raw), len(todo))
    for index, ad_id in enumerate(todo, 1):
        try:
            raw[ad_id] = crawl_detail(session, ad_id, config)
            metric_inc("crawled")
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                logger.info("Listing %s không còn tồn tại", ad_id)
                all_ids.discard(int(ad_id) if ad_id.isdigit() else ad_id)
            else:
                logger.error("HTTP error %s: %s", ad_id, error)
                metric_inc("errors", stage="detail")
            continue
        except requests.RequestException as error:
            logger.error("Request error %s: %s", ad_id, error)
            metric_inc("errors", stage="detail")
            continue

        if index % 20 == 0:
            save_json(RAW_FILE, raw)
            logger.info("Checkpoint %d/%d (tổng %d)", index, len(todo), len(raw))
        time.sleep(config.sleep_detail)

    save_json(RAW_FILE, raw)
    save_json(ID_FILE, sorted(all_ids, key=str))
    metric_set("stored_records", len(raw))
    logger.info("Hoàn tất: %d records trong %s", len(raw), RAW_FILE)
    return raw


def run_once(session: requests.Session, config: CrawlerConfig) -> None:
    all_ids = step_collect_ids(session, config)
    step_crawl_details(session, all_ids, config)


def parse_args() -> argparse.Namespace:
    default_regions = tuple(
        int(value.strip())
        for value in os.getenv("CRAWLER_REGIONS", "12000").split(",")
        if value.strip()
    )
    parser = argparse.ArgumentParser(description="Crawl dữ liệu BĐS Chợ Tốt")
    parser.add_argument("--region", type=int, action="append", dest="regions")
    parser.add_argument(
        "--category", type=int, default=int(os.getenv("CRAWLER_CATEGORY", "1000"))
    )
    parser.add_argument(
        "--page-start", type=int, default=int(os.getenv("CRAWLER_PAGE_START", "0"))
    )
    parser.add_argument(
        "--page-end", type=int, default=int(os.getenv("CRAWLER_PAGE_END", "3"))
    )
    parser.add_argument(
        "--limit", type=int, default=int(os.getenv("CRAWLER_LIMIT", "20"))
    )
    parser.add_argument("--watch", action="store_true", help="Lặp crawl định kỳ")
    parser.add_argument(
        "--interval", type=int, default=int(os.getenv("CRAWLER_INTERVAL", "900"))
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.getenv("CRAWLER_METRICS_PORT", "9102")),
    )
    args = parser.parse_args()
    args.regions = tuple(args.regions or default_regions)
    if args.page_end <= args.page_start:
        parser.error("--page-end phải lớn hơn --page-start")
    return args


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args()
    config = CrawlerConfig(
        regions=args.regions,
        category=args.category,
        page_start=args.page_start,
        page_end=args.page_end,
        limit=args.limit,
    )
    configure_metrics(args.metrics_port)
    session = create_session()
    try:
        while True:
            run_once(session, config)
            if not args.watch:
                break
            logger.info("Chu kỳ tiếp theo sau %d giây", args.interval)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Đã dừng crawler")
    finally:
        session.close()


if __name__ == "__main__":
    main()
