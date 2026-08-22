import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "dashboard"))

from data_access import IncrementalWebHdfsCache  # noqa: E402


class _Response:
    def __init__(self, *, status=200, payload=None, content=b"", headers=None):
        self.status_code = status
        self._payload = payload or {}
        self.content = content
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Session:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        return self.responses[url]


class IncrementalWebHdfsCacheTests(unittest.TestCase):
    def test_second_sync_reuses_unchanged_parquet_file(self):
        base = "http://namenode/webhdfs/v1/data/real-estate"
        batch_url = f"{base}/batch_id%3D0001"
        type_url = f"{batch_url}/property_type%3DNh%C3%A0"
        file_url = f"{type_url}/part-000.parquet"
        redirect_url = "http://datanode:9864/webhdfs/v1/file?op=OPEN"
        local_redirect = "http://localhost:9864/webhdfs/v1/file?op=OPEN"
        responses = {
            f"{base}?op=LISTSTATUS": _Response(payload={"FileStatuses": {"FileStatus": [
                {"pathSuffix": "batch_id=0001", "type": "DIRECTORY"}
            ]}}),
            f"{batch_url}?op=LISTSTATUS": _Response(payload={"FileStatuses": {"FileStatus": [
                {"pathSuffix": "property_type=Nhà", "type": "DIRECTORY"}
            ]}}),
            f"{type_url}?op=LISTSTATUS": _Response(payload={"FileStatuses": {"FileStatus": [
                {"pathSuffix": "part-000.parquet", "type": "FILE", "length": 9, "modificationTime": 100}
            ]}}),
            f"{file_url}?op=OPEN": _Response(status=307, headers={"Location": redirect_url}),
            local_redirect: _Response(content=b"parquet-1"),
        }
        session = _Session(responses)

        with tempfile.TemporaryDirectory() as temp_dir:
            cache = IncrementalWebHdfsCache(base, Path(temp_dir) / "cache", session=session)
            first_path = cache.sync()
            second_path = cache.sync()

            parquet = first_path / "batch_id=0001" / "property_type=Nhà" / "part-000.parquet"
            self.assertEqual(first_path, second_path)
            self.assertEqual(parquet.read_bytes(), b"parquet-1")
            self.assertEqual(session.calls.count(local_redirect), 1)
            self.assertTrue((first_path / ".manifest.json").exists())

            cache.clear()
            self.assertFalse(first_path.exists())


@unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas is not installed")
class DashboardCalculationTests(unittest.TestCase):
    def test_missing_and_zero_area_do_not_create_fake_unit_prices(self):
        import pandas as pd
        from data_access import prepare_dataframe

        raw = pd.DataFrame(
            {
                "property_type": ["Nhà", None, "Đất"],
                "district": ["Ba Đình", None, ""],
                "price": [2_000_000_000, None, 1_000_000_000],
                "area_m2": [100, 50, 0],
            }
        )
        result = prepare_dataframe(raw)

        self.assertEqual(result.loc[0, "price_per_m2_trieu"], 20)
        self.assertTrue(pd.isna(result.loc[1, "price"]))
        self.assertTrue(pd.isna(result.loc[1, "price_per_m2_trieu"]))
        self.assertTrue(pd.isna(result.loc[2, "price_per_m2_trieu"]))
        self.assertEqual(result.loc[1, "property_type"], "Khác")


if __name__ == "__main__":
    unittest.main()
