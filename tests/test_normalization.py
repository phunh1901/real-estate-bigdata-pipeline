import copy
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "kafka"))

import traffic_ingestion as ingestion  # noqa: E402


class NormalizeAdTests(unittest.TestCase):
    def setUp(self):
        self.raw = {
            "ad": {
                "subject": "Căn hộ trung tâm",
                "body": "Hai phòng ngủ",
                "price": 3_500_000_000,
                "price_string": "3,5 tỷ",
                "list_time": 1_704_067_200_000,
                "category_name": "Căn hộ/Chung cư",
                "region_name": "Hà Nội",
                "area_name": "Cầu Giấy",
                "params": [
                    {"id": "size", "value": "72.5 m²"},
                    {"id": "rooms", "value": "2 phòng"},
                    {"id": "toilets", "value": 2},
                ],
            }
        }

    def test_normalize_ad_maps_nested_parameters(self):
        record = ingestion.normalize_ad("123", self.raw)

        self.assertEqual(record["list_id"], "123")
        self.assertEqual(record["title"], "Căn hộ trung tâm")
        self.assertEqual(record["listing_type"], "Bán")
        self.assertEqual(record["property_type"], "Căn hộ/Chung cư")
        self.assertEqual(record["price"], 3_500_000_000.0)
        self.assertEqual(record["area_m2"], 72.5)
        self.assertEqual(record["rooms"], 2)
        self.assertEqual(record["toilets"], 2)
        self.assertEqual(record["district"], "Cầu Giấy")
        self.assertEqual(record["posted_at"], "2024-01-01")
        self.assertIn("Căn hộ trung tâm", record["full_text"])

    def test_normalize_ad_detects_monthly_rental(self):
        self.raw["ad"]["price_string"] = "15 triệu/tháng"
        self.assertEqual(ingestion.normalize_ad("124", self.raw)["listing_type"], "Cho thuê")

    def test_normalize_ad_does_not_mutate_raw_input(self):
        before = copy.deepcopy(self.raw)
        ingestion.normalize_ad("125", self.raw)
        self.assertEqual(self.raw, before)

    def test_safe_numeric_conversion(self):
        self.assertEqual(ingestion.to_float("1,234.5 m²"), 1234.5)
        self.assertEqual(ingestion.to_int("3 phòng"), 3)
        self.assertIsNone(ingestion.to_float(None))
        self.assertIsNone(ingestion.to_float("không rõ"))


if __name__ == "__main__":
    unittest.main()
