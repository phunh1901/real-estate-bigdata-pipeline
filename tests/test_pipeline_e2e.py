import io
import os
import sys
import time
import unittest
import urllib.parse
import uuid
from pathlib import Path

try:
    import pytest

    pytestmark = pytest.mark.integration
except ImportError:
    # unittest vẫn có thể discover file này; live test mặc định sẽ được skip.
    pass


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "kafka"))

import traffic_ingestion as ingestion  # noqa: E402


RUN_E2E = os.getenv("RUN_PIPELINE_INTEGRATION") == "1"
WEBHDFS_URL = os.getenv(
    "WEBHDFS_URL", "http://localhost:9870/webhdfs/v1/data/real-estate"
).rstrip("/")


def _list_parquet_files(url):
    import requests

    response = requests.get(f"{url}?op=LISTSTATUS", timeout=10)
    response.raise_for_status()
    files = []
    for item in response.json().get("FileStatuses", {}).get("FileStatus", []):
        item_url = f"{url}/{urllib.parse.quote(item['pathSuffix'])}"
        if item["type"] == "DIRECTORY":
            files.extend(_list_parquet_files(item_url))
        elif item["pathSuffix"].endswith(".parquet"):
            files.append(item_url)
    return files


def _download_parquet(url):
    import pandas as pd
    import requests

    first = requests.get(f"{url}?op=OPEN", allow_redirects=False, timeout=10)
    first.raise_for_status()
    location = first.headers["Location"]
    parsed = urllib.parse.urlsplit(location)
    local_location = urllib.parse.urlunsplit(
        (parsed.scheme, f"localhost:{parsed.port}", parsed.path, parsed.query, parsed.fragment)
    )
    content = requests.get(local_location, timeout=20)
    content.raise_for_status()
    return pd.read_parquet(io.BytesIO(content.content))


@unittest.skipUnless(RUN_E2E, "set RUN_PIPELINE_INTEGRATION=1 to run Docker E2E")
class DockerPipelineEndToEndTests(unittest.TestCase):
    def test_kafka_record_reaches_hdfs_parquet(self):
        list_id = f"e2e-{uuid.uuid4()}"
        raw = {
            "ad": {
                "subject": "E2E pipeline record",
                "body": "Temporary automated test record",
                "price": 1_000_000_000,
                "price_string": "1 tỷ",
                "category_name": "Test",
                "area_name": "Test District",
                "size": 50,
            }
        }
        record = ingestion.normalize_ad(list_id, raw)
        producer = ingestion.create_producer()
        try:
            producer.send(
                ingestion.config.KAFKA_TOPIC, key=list_id, value=record
            ).get(timeout=10)
            producer.flush()
        finally:
            producer.close()

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            for parquet_url in _list_parquet_files(WEBHDFS_URL):
                frame = _download_parquet(parquet_url)
                if "list_id" in frame and list_id in set(frame["list_id"].astype(str)):
                    return
            time.sleep(5)

        self.fail(f"Record {list_id} did not reach HDFS within 90 seconds")


if __name__ == "__main__":
    unittest.main()
