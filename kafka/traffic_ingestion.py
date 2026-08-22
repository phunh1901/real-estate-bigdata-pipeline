"""
Đẩy dữ liệu bất động sản lên Kafka.
...
"""
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import os

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RAW_FILE = Path(
    os.getenv(
        "RAW_FILE",
        str(Path(__file__).parent.parent / "crawler" / "data" / "all_raw_data.json"),
    )
)
SENT_IDS_FILE = Path(
    os.getenv("SENT_IDS_FILE", str(Path(__file__).parent / ".sent_ids.json"))
)

BATCH_SIZE = 100

#  ép kiểu an toàn

def to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    digits = "".join(ch for ch in str(value) if ch in "0123456789.,")
    digits = digits.replace(",", "")
    try:
        return float(digits) if digits.strip(".") else None
    except ValueError:
        return None


def to_int(value):
    f = to_float(value)
    return int(f) if f is not None else None


def build_param_lookup(raw: dict) -> dict:
    lookup = {}
    ad = raw.get("ad", raw)
    sources = [
        ad.get("params") if isinstance(ad, dict) else None,
        raw.get("ad_params"),
        raw.get("parameters"),
    ]
    for src in sources:
        if isinstance(src, dict):
            for key, obj in src.items():
                val = obj.get("value") if isinstance(obj, dict) else obj
                if val not in (None, "", []):
                    lookup.setdefault(key, val)
        elif isinstance(src, list):
            for p in src:
                if isinstance(p, dict) and "id" in p:
                    val = p.get("value")
                    if val not in (None, "", []):
                        lookup.setdefault(p["id"], val)
    return lookup


# ----------------------------------------------------------------------------- #
# Chuẩn hoá 1 tin rao
# ----------------------------------------------------------------------------- #
def normalize_ad(list_id, raw: dict) -> dict:
    ad = raw.get("ad", raw)
    params = build_param_lookup(raw)

    def pick(*keys):
        for k in keys:
            v = ad.get(k)
            if v not in (None, "", []):
                return v
        for k in keys:
            v = params.get(k)
            if v not in (None, "", []):
                return v
        return None

    title = (pick("subject") or "").strip()
    description = pick("body") or ""
    price_text = pick("price_string") or ""
    listing_type = "Cho thuê" if "tháng" in price_text.lower() else "Bán"

    posted_at = ""
    lt = pick("list_time")
    if lt is not None:
        ts = to_float(lt)
        if ts is not None:
            if ts > 1e12:
                ts = ts / 1000.0
            try:
                posted_at = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            except (OverflowError, OSError, ValueError):
                posted_at = str(lt)

    record = {
        "list_id":       str(list_id),
        "title":         title,
        "description":   description,
        "listing_type":  listing_type,
        "property_type": pick("category_name") or "Khác",
        "price":         to_float(pick("price")),
        "price_text":    price_text,
        "area_m2":       to_float(pick("size", "area", "living_size")),
        "rooms":         to_int(pick("rooms")),
        "toilets":       to_int(pick("toilets")),
        "region":        pick("region_name") or "",
        "district":      pick("area_name") or "",
        "ward":          pick("ward_name") or "",
        "street":        (pick("street_name", "street_number") or "").strip(),
        "latitude":      to_float(pick("latitude")),
        "longitude":     to_float(pick("longitude")),
        "posted_at":     posted_at,
        "url":           f"https://www.chotot.com/{list_id}.htm",
    }
    record["full_text"] = (title + "\n" + description).strip()
    return record


# Track sent IDs — đọc/ghi file local để dedup giữa các lần chạy
def load_sent_ids() -> set:
    """Đọc danh sách list_id đã gửi thành công từ file local."""
    if not SENT_IDS_FILE.exists():
        return set()
    try:
        return set(json.loads(SENT_IDS_FILE.read_text(encoding="utf-8")))
    except Exception:
        return set()


def save_sent_ids(sent_ids: set) -> None:
    """Ghi lại danh sách list_id đã gửi ra file local."""
    SENT_IDS_FILE.write_text(
        json.dumps(list(sent_ids), ensure_ascii=False),
        encoding="utf-8",
    )


# Kafka
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        client_id=config.KAFKA_CLIENT_ID,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,

        acks="all",         # Đảm bảo broker xác nhận trước khi tiếp tục
        retries=5,
        retry_backoff_ms=500,

        linger_ms=10,
        batch_size=16384,
        compression_type="gzip",
    )


def load_raw() -> dict:
    if not RAW_FILE.exists():
        logger.error("Không tìm thấy %s. Hãy chạy crawler trước.", RAW_FILE)
        return {}
    return json.loads(RAW_FILE.read_text(encoding="utf-8"))


# Gửi data với dedup
def send_all(producer: KafkaProducer, raw_map: dict) -> int:
    # Load IDs đã gửi từ lần chạy trước
    sent_ids = load_sent_ids()

    # Lọc ra chỉ những ID chưa gửi
    new_items = [(lid, raw) for lid, raw in raw_map.items() if str(lid) not in sent_ids]

    if not new_items:
        logger.info("Tất cả %d records đã được gửi trước đó. Không có gì mới.", len(raw_map))
        return 0

    logger.info(
        "Tổng: %d | Đã gửi trước đó: %d | Cần gửi mới: %d",
        len(raw_map),
        len(sent_ids),
        len(new_items),
    )

    success = 0
    total_new = len(new_items)
    errors = []

    for batch_start in range(0, total_new, BATCH_SIZE):
        batch = new_items[batch_start : batch_start + BATCH_SIZE]
        batch_sent_ids = set()

        for list_id, raw in batch:
            try:
                record = normalize_ad(list_id, raw)
            except Exception as e:
                logger.warning("Bỏ qua %s do lỗi normalize: %s", list_id, e)
                continue

            try:
                future = producer.send(
                    config.KAFKA_TOPIC,
                    key=record["property_type"],
                    value=record,
                )
                future.get(timeout=10)  # Chờ xác nhận từ broker
                batch_sent_ids.add(str(list_id))
            except Exception as e:
                logger.error("Lỗi khi gửi %s: %s", list_id, e)
                errors.append(list_id)

        # Flush batch trước khi lưu
        producer.flush()

        # Lưu IDs đã gửi thành công
        sent_ids.update(batch_sent_ids)
        save_sent_ids(sent_ids)
        success += len(batch_sent_ids)

        logger.info(
            "Batch %d-%d: gửi %d records | Tổng đã gửi: %d/%d",
            batch_start + 1,
            batch_start + len(batch),
            len(batch_sent_ids),
            success,
            total_new,
        )

    if errors:
        logger.warning("Có %d records gửi thất bại, sẽ retry lần sau.", len(errors))

    return success


def inspect(raw_map: dict) -> None:
    if not raw_map:
        return
    first_id, first_raw = next(iter(raw_map.items()))
    ad = first_raw.get("ad", first_raw)
    print("== Các key trong 'ad' ==")
    print(sorted(ad.keys()) if isinstance(ad, dict) else type(ad))
    print("\n== Bản ghi sau khi chuẩn hoá ==")
    print(json.dumps(normalize_ad(first_id, first_raw), ensure_ascii=False, indent=2))


def main() -> None:
    raw_map = load_raw()
    if not raw_map:
        return

    if "--inspect" in sys.argv:
        inspect(raw_map)
        return

    # --reset: xóa file tracking để push lại toàn bộ data từ đầu
    if "--reset" in sys.argv:
        if SENT_IDS_FILE.exists():
            SENT_IDS_FILE.unlink()
            logger.info("Đã xóa file tracking. Sẽ gửi lại toàn bộ data.")

    logger.info("Topic: %s | Bootstrap: %s", config.KAFKA_TOPIC, config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Tổng số tin trong file thô: %d", len(raw_map))

    producer = None
    try:
        producer = create_producer()
        logger.info("Kết nối Kafka thành công!")
        sent = send_all(producer, raw_map)
        logger.info("Hoàn thành! Đã gửi %d tin mới lên Kafka.", sent)
    except Exception as e:
        logger.error("Lỗi: %s. Kiểm tra Kafka đã chạy chưa (docker-compose up).", e)
    finally:
        if producer:
            producer.close()
            logger.info("Đã đóng kết nối Kafka.")


if __name__ == "__main__":
    main()
