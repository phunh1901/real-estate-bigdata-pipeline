import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "kafka"))

import traffic_ingestion as ingestion  # noqa: E402


class _AcknowledgedSend:
    def get(self, timeout=None):
        return {"timeout": timeout}


class _RecordingProducer:
    def __init__(self):
        self.records = []
        self.flush_count = 0

    def send(self, topic, key, value):
        self.records.append((topic, key, value))
        return _AcknowledgedSend()

    def flush(self):
        self.flush_count += 1


class IngestionIntegrationTests(unittest.TestCase):
    def test_raw_records_are_normalized_sent_and_deduplicated(self):
        raw_map = {
            "listing-1": {
                "ad": {
                    "subject": "Nhà mặt phố",
                    "body": "Dữ liệu integration test",
                    "price": 8_000_000_000,
                    "price_string": "8 tỷ",
                    "category_name": "Nhà ở",
                    "area_name": "Ba Đình",
                    "size": 60,
                }
            }
        }
        producer = _RecordingProducer()

        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "state" / "sent_ids.json"
            with patch.object(ingestion, "SENT_IDS_FILE", state_file):
                first_count = ingestion.send_all(producer, raw_map)
                second_count = ingestion.send_all(producer, raw_map)

                self.assertEqual(first_count, 1)
                self.assertEqual(second_count, 0)
                self.assertEqual(len(producer.records), 1)
                topic, key, record = producer.records[0]
                self.assertEqual(topic, ingestion.config.KAFKA_TOPIC)
                self.assertEqual(key, "listing-1")
                self.assertEqual(record["list_id"], "listing-1")
                self.assertEqual(ingestion.load_sent_ids(), {"listing-1"})
                self.assertFalse(state_file.with_name("sent_ids.json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
