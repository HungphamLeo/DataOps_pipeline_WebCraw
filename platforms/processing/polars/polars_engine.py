"""
platforms/processing/polars/polars_engine.py
============================================
Layer: Platform Processing — Polars engine wrapper.
Responsibility: Wrap Polars I/O với S3/MinIO storage_options.
Does NOT contain: domain logic, pipeline business rules.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PolarsConfig:
    storage_options: Dict[str, str] = field(default_factory=dict)
    thread_pool_size: Optional[int] = None
    enable_streaming: bool = True


class PolarsEngine:
    """Thin wrapper around Polars providing S3-aware read/write Parquet."""

    def __init__(self, config: PolarsConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        if config.thread_pool_size:
            try:
                import polars as pl
                pl.Config.set_tbl_rows(20)
            except Exception:
                pass

    def _build_pyarrow_s3_filesystem(self):
        """Build a PyArrow S3 filesystem for MinIO/S3 writes when endpoint creds exist."""
        if not self.config.storage_options:
            return None

        endpoint = self.config.storage_options.get("endpoint_url")
        if not endpoint:
            return None

        try:
            import pyarrow.fs as pafs
        except Exception:
            return None

        access_key = self.config.storage_options.get("aws_access_key_id")
        secret_key = self.config.storage_options.get("aws_secret_access_key")
        endpoint_host = endpoint.replace("http://", "").replace("https://", "")
        scheme = "https" if endpoint.startswith("https://") else "http"

        try:
            return pafs.S3FileSystem(
                endpoint_override=endpoint_host,
                access_key=access_key,
                secret_key=secret_key,
                region="us-east-1",
                scheme=scheme,
                anonymous=False,
            )
        except Exception as exc:
            self.logger.warning("[Polars] Unable to build S3 filesystem: %s", exc)
            return None

    def write_parquet(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]] = None,
    ) -> str:
        """Write Polars DataFrame as Parquet to local path or S3. This method is compatible with Polars 1.12.x."""

        pyarrow_options = {}
        filesystem = self._build_pyarrow_s3_filesystem()
        if filesystem is not None:
            pyarrow_options["filesystem"] = filesystem

        kwargs: Dict[str, Any] = {
            "use_pyarrow": True,
            "pyarrow_options": pyarrow_options or None,
        }

        if partition_by:
            kwargs["partition_by"] = partition_by
            # Polars 1.12.x accepts partition_by in the public API; we do not pass storage_options.

        df.write_parquet(target_path, **kwargs)
        self.logger.debug("[Polars] Wrote %d rows → %s", len(df), target_path)
        return target_path

    def read_parquet(self, path: str) -> Any:
        """Read Parquet from local path or S3."""
        import polars as pl
        return pl.read_parquet(path, storage_options=self.config.storage_options or None)
