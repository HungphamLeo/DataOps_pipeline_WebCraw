"""
platforms/processing/polars/polars_engine.py
============================================
Layer: Platform Processing — Polars engine wrapper.
Responsibility: Wrap Polars I/O với S3/MinIO storage_options.
Does NOT contain: domain logic, pipeline business rules.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

def _extract_bucket(s3_path: str) -> Optional[str]:
    """Extract bucket name từ s3://bucket/... hoặc s3a://bucket/..."""
    if not s3_path.startswith(("s3://", "s3a://")):
        return None
    without_scheme = s3_path.split("://", 1)[1]
    return without_scheme.split("/")[0] or None

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
    

    
    def _ensure_s3_bucket(self, s3_path: str) -> None:
        """Tạo bucket trên MinIO/S3 nếu chưa tồn tại, dùng s3fs."""
        bucket = _extract_bucket(s3_path)
        if not bucket:
            return
        opts = self.config.storage_options
        if not opts.get("endpoint_url"):
            return
        try:
            import s3fs
            fs = s3fs.S3FileSystem(
                endpoint_url=opts["endpoint_url"],
                key=opts.get("aws_access_key_id"),
                secret=opts.get("aws_secret_access_key"),
                use_ssl=False,
            )
            if not fs.exists(bucket):
                fs.mkdir(bucket)
                self.logger.info("[Polars] Created S3 bucket: %s", bucket)
        except Exception as exc:
            self.logger.warning("[Polars] Could not ensure bucket '%s': %s", bucket, exc)

    def _build_pyarrow_s3_filesystem(self):
        """Build a PyArrow filesystem for S3-compatible storage.

        PyArrow requires the endpoint without its URL scheme, whereas the
        application configuration stores it as a URL.  For normal AWS S3
        operation, returning ``None`` preserves PyArrow's default behaviour.
        """
        options = self.config.storage_options
        if not options:
            return None

        endpoint = options.get("endpoint_url") or options.get("endpoint")
        if not endpoint:
            return None

        try:
            import pyarrow.fs as pafs
        except Exception:
            return None

        endpoint = endpoint.rstrip("/")
        scheme = "https" if endpoint.lower().startswith("https://") else "http"
        endpoint_host = endpoint.split("://", 1)[-1]
        access_key = options.get("aws_access_key_id") or options.get("access_key")
        secret_key = options.get("aws_secret_access_key") or options.get("secret_key")

        try:
            filesystem_options = {
                "endpoint_override": endpoint_host,
                "access_key": access_key,
                "secret_key": secret_key,
                "region": options.get("region_name", "us-east-1"),
                "scheme": scheme,
                "anonymous": not bool(access_key and secret_key),
            }
            # S3-compatible services commonly require path-style addressing.
            if "force_virtual_addressing" in options:
                filesystem_options["force_virtual_addressing"] = bool(
                    options["force_virtual_addressing"]
                )
            return pafs.S3FileSystem(**filesystem_options)
        except Exception as exc:
            self.logger.warning("[Polars] Unable to build S3 filesystem: %s", exc)
            return None

    def write_parquet(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]] = None,
    ) -> str:
        """Write a DataFrame to a Parquet file or partitioned dataset.

        A partitioned write targets a directory because PyArrow creates one
        or more files below each partition.  A non-partitioned write must
        target a regular file, so directory-style paths are converted to a
        deterministic ``part-0.parquet`` path.
        """
        if target_path.startswith(("s3://", "s3a://")):
            self._ensure_s3_bucket(target_path)
            filesystem = self._build_pyarrow_s3_filesystem()
            if filesystem is None:
                raise RuntimeError(
                    "Cannot write S3 Parquet: PyArrow S3 filesystem could not be built"
                )
            self._write_with_pyarrow(
                df=df,
                target_path=target_path,
                partition_by=partition_by,
                filesystem=filesystem,
            )
            self.logger.debug("[Polars] Wrote %d rows → %s", len(df), target_path)
            return target_path

        write_path = target_path
        if not partition_by:
            write_path = self._resolve_file_path(target_path)
        else:
            Path(write_path).mkdir(parents=True, exist_ok=True)

        df.write_parquet(write_path, partition_by=partition_by)
        self.logger.debug("[Polars] Wrote %d rows → %s", len(df), write_path)
        return write_path

    def _write_with_pyarrow(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]],
        filesystem: Any,
    ) -> None:
        """Write a local or S3 dataset without passing directories to a file writer."""
        import pyarrow.dataset as pads
        import pyarrow.parquet as papq

        table = df.to_arrow()
        storage_path = target_path.split("://", 1)[-1].rstrip("/")

        if partition_by:
            pads.write_dataset(
                table,
                base_dir=storage_path,
                filesystem=filesystem,
                format="parquet",
                partitioning=partition_by,
                basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
                existing_data_behavior="overwrite_or_ignore",
            )
            return

        file_path = f"{storage_path}/part-0.parquet"
        papq.write_table(table, file_path, filesystem=filesystem)

    @staticmethod
    def _resolve_file_path(target_path: str) -> str:
        """Convert a directory-style target into a concrete Parquet file."""
        if target_path.endswith(".parquet"):
            return target_path
        return f"{target_path.rstrip('/')}/part-0.parquet"

    def read_parquet(self, path: str) -> Any:
        """Read Parquet from local path or S3."""
        import polars as pl
        return pl.read_parquet(path, storage_options=self.config.storage_options or None)
