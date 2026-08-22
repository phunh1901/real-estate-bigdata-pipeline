"""Đọc và chuẩn hóa dataset cho dashboard, độc lập với Streamlit UI."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import urllib.parse
from pathlib import Path, PurePosixPath

REQUIRED_COLUMNS = [
    "list_id",
    "title",
    "property_type",
    "district",
    "price",
    "area_m2",
    "rooms",
    "url",
    "listing_type",
]


def prepare_dataframe(raw_df):
    """Chuẩn hóa kiểu dữ liệu mà không biến giá trị thiếu thành số 0 giả."""
    import pandas as pd

    df = raw_df.copy()
    df["property_type"] = (
        df["property_type"]
        .astype("string")
        .fillna("Khác")
        .replace({"": "Khác", "nan": "Khác", "None": "Khác", "<NA>": "Khác"})
    )
    df["district"] = (
        df["district"]
        .astype("string")
        .fillna("")
        .replace({"nan": "", "None": "", "<NA>": ""})
    )
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["area_m2"] = pd.to_numeric(df["area_m2"], errors="coerce")
    df["price_ty"] = df["price"] / 1e9
    df["price_trieu"] = df["price"] / 1e6

    valid_unit_price = (df["price"] > 0) & (df["area_m2"] > 0)
    df["price_per_m2_trieu"] = (
        (df["price"] / df["area_m2"] / 1e6).where(valid_unit_price)
    )
    return df


class IncrementalWebHdfsCache:
    """Đồng bộ tăng dần các file Parquet từ WebHDFS xuống cache cục bộ."""

    def __init__(
        self,
        base_url: str,
        cache_root: str | Path | None = None,
        datanode_host: str = "localhost",
        session=None,
    ):
        self.base_url = base_url.rstrip("/")
        cache_key = hashlib.sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        default_root = Path(tempfile.gettempdir()) / "bigdata_real_estate_dashboard" / cache_key
        self.cache_root = Path(cache_root) if cache_root else default_root
        self.datanode_host = datanode_host
        if session is None:
            import requests

            session = requests.Session()
        self.session = session
        self.manifest_path = self.cache_root / ".manifest.json"

    def _safe_local_path(self, relative_path: str) -> Path:
        parts = PurePosixPath(relative_path).parts
        if not parts or any(part in {"", ".", ".."} for part in parts):
            raise ValueError(f"Đường dẫn HDFS không an toàn: {relative_path}")
        root = self.cache_root.resolve()
        candidate = (root / Path(*parts)).resolve()
        if root not in candidate.parents:
            raise ValueError(f"Đường dẫn cache vượt ngoài phạm vi: {relative_path}")
        return candidate

    def _list_files(self, url: str, relative_dir: str = "") -> dict[str, dict]:
        response = self.session.get(f"{url}?op=LISTSTATUS", timeout=15)
        response.raise_for_status()
        statuses = response.json().get("FileStatuses", {}).get("FileStatus", [])
        files = {}
        for item in statuses:
            suffix = item.get("pathSuffix", "")
            if not suffix or suffix in {"_SUCCESS", "_staging"}:
                continue
            relative_path = f"{relative_dir}/{suffix}".strip("/")
            item_url = f"{url}/{urllib.parse.quote(suffix, safe='')}"
            if item["type"] == "DIRECTORY":
                files.update(self._list_files(item_url, relative_path))
            elif item["type"] == "FILE" and suffix.endswith(".parquet"):
                files[relative_path] = {
                    "url": item_url,
                    "length": int(item.get("length", 0)),
                    "modification_time": int(item.get("modificationTime", 0)),
                }
        return files

    def _load_manifest(self) -> dict[str, dict]:
        if not self.manifest_path.exists():
            return {}
        try:
            return json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}

    def _save_manifest(self, manifest: dict[str, dict]) -> None:
        self.cache_root.mkdir(parents=True, exist_ok=True)
        temp_path = self.manifest_path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(temp_path, self.manifest_path)

    def _download(self, remote_url: str, destination: Path) -> None:
        first = self.session.get(
            f"{remote_url}?op=OPEN", allow_redirects=False, timeout=15
        )
        first.raise_for_status()
        if first.status_code in {301, 302, 303, 307, 308}:
            location = first.headers["Location"]
            parsed = urllib.parse.urlsplit(location)
            port = f":{parsed.port}" if parsed.port else ""
            location = urllib.parse.urlunsplit(
                (
                    parsed.scheme,
                    f"{self.datanode_host}{port}",
                    parsed.path,
                    parsed.query,
                    parsed.fragment,
                )
            )
            content = self.session.get(location, timeout=30)
            content.raise_for_status()
        else:
            content = first

        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = destination.with_suffix(f"{destination.suffix}.part")
        temp_path.write_bytes(content.content)
        os.replace(temp_path, destination)

    def sync(self) -> Path:
        remote_files = self._list_files(self.base_url)
        if not remote_files:
            raise FileNotFoundError(f"Không có file Parquet tại {self.base_url}")

        old_manifest = self._load_manifest()
        new_manifest = {}
        for relative_path, metadata in remote_files.items():
            local_path = self._safe_local_path(relative_path)
            signature = {
                "length": metadata["length"],
                "modification_time": metadata["modification_time"],
            }
            if old_manifest.get(relative_path) != signature or not local_path.exists():
                self._download(metadata["url"], local_path)
            new_manifest[relative_path] = signature

        for stale_path in set(old_manifest) - set(new_manifest):
            local_path = self._safe_local_path(stale_path)
            if local_path.exists():
                local_path.unlink()

        self._save_manifest(new_manifest)
        return self.cache_root

    def clear(self) -> None:
        if self.cache_root.exists():
            shutil.rmtree(self.cache_root)


def load_dataset(
    data_source: str,
    local_path: str | Path,
    webhdfs_url: str,
    cache_path: str | Path | None = None,
    datanode_host: str = "localhost",
):
    import pandas as pd

    if data_source == "local":
        dataset_path = Path(local_path)
        if not dataset_path.exists():
            raise FileNotFoundError(f"Không tìm thấy dữ liệu local: {dataset_path}")
    elif data_source == "hdfs":
        dataset_path = IncrementalWebHdfsCache(
            webhdfs_url, cache_path, datanode_host
        ).sync()
    else:
        raise ValueError("DASHBOARD_DATA_SOURCE phải là 'hdfs' hoặc 'local'")

    if not any(dataset_path.rglob("*.parquet")):
        raise FileNotFoundError(f"Không có file Parquet trong {dataset_path}")
    raw_df = pd.read_parquet(
        dataset_path, engine="pyarrow", columns=REQUIRED_COLUMNS
    )
    return prepare_dataframe(raw_df)
