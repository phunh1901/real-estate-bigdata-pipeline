import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "crawler"))

REQUESTS_AVAILABLE = importlib.util.find_spec("requests") is not None


@unittest.skipUnless(REQUESTS_AVAILABLE, "requests is not installed")
class CrawlerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global crawler
        import crawl as crawler_module

        crawler = crawler_module

    def test_atomic_json_checkpoint_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "checkpoint.json"
            crawler.save_json(path, {"ids": [3, 1, 2]})

            self.assertEqual(crawler.load_json(path, {}), {"ids": [3, 1, 2]})
            self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_collect_ids_merges_regions_without_duplicates(self):
        config = crawler.CrawlerConfig(
            regions=(12000, 13000), page_start=0, page_end=1, sleep_list=0
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            id_file = Path(temp_dir) / "ids.json"
            id_file.write_text(json.dumps([1]), encoding="utf-8")
            with (
                patch.object(crawler, "ID_FILE", id_file),
                patch.object(crawler, "get_ad_ids", side_effect=[[1, 2], [2, 3]]),
            ):
                result = crawler.step_collect_ids(object(), config)

            self.assertEqual(result, {1, 2, 3})
            self.assertEqual(set(json.loads(id_file.read_text(encoding="utf-8"))), {1, 2, 3})


if __name__ == "__main__":
    unittest.main()
