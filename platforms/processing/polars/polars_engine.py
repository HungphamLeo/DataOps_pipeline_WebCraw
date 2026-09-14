"""
platforms/processing/polars/polars_engine.py
============================================
Layer: Platform Processing — Polars engine wrapper.
Responsibility: Wrap Polars I/O với S3/MinIO storage_options.
Does NOT contain: domain logic, pipeline business rules.

S3/MinIO write strategy (Polars 1.12 + PyArrow 17):
  - Polars df.write_parquet(..., use_pyarrow=True, storage_options=...) truyền
    storage_options sang pyarrow.fs.S3FileSystem — nhưng pyarrow.fs.S3FileSystem
    không nhận endpoint_url / aws_access_key_id trực tiếp → gây lỗi `use_ssl`.
  - Fix: dùng s3fs.S3FileSystem trực tiếp để build filesystem, sau đó ghi qua
    pyarrow.dataset.write_to_dataset (cho partitioned) hoặc pyarrow.parquet.write_table
    (cho non-partitioned). Polars df được convert sang pyarrow.Table trước khi ghi.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


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

    # ------------------------------------------------------------------
    # Internal: build s3fs filesystem từ storage_options
    # ------------------------------------------------------------------

    def _build_s3fs(self):
        """
        Tạo s3fs.S3FileSystem từ storage_options dict.
        storage_options keys: endpoint_url, aws_access_key_id, aws_secret_access_key.
        Trả về (fs, bucket, key_prefix) hoặc None nếu không phải S3 path.
        """
        try:
            import s3fs
            opts = self.config.storage_options or {}
            endpoint_url = opts.get("endpoint_url", "")
            fs = s3fs.S3FileSystem(
                key=opts.get("aws_access_key_id"),
                secret=opts.get("aws_secret_access_key"),
                endpoint_url=endpoint_url or None,
                use_ssl=endpoint_url.startswith("https://") if endpoint_url else False,
            )
            return fs
        except Exception as exc:
            self.logger.warning("[Polars] Unable to build S3 filesystem: %s", exc)
            return None

    @staticmethod
    def _is_s3_path(path: str) -> bool:
        return path.startswith("s3://") or path.startswith("s3a://")

    # ------------------------------------------------------------------
    # write_parquet
    # ------------------------------------------------------------------

    def write_parquet(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]] = None,
    ) -> str:
        """Write Polars DataFrame as Parquet to local path or S3/MinIO."""
        if self._is_s3_path(target_path):
            self._write_parquet_s3(df, target_path, partition_by)
        else:
            self._write_parquet_local(df, target_path, partition_by)
        self.logger.debug("[Polars] Wrote %d rows → %s", len(df), target_path)
        return target_path

    def _write_parquet_s3(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]],
    ) -> None:
        """Ghi Parquet lên S3/MinIO qua s3fs + pyarrow.dataset."""
        import pyarrow.dataset as pad
        import pyarrow.parquet as pq

        fs = self._build_s3fs()
        if fs is None:
            raise RuntimeError(
                f"[Polars] Cannot build S3 filesystem — check storage_options. path={target_path}"
            )

        # Strip scheme để pyarrow dataset nhận path dạng bucket/key
        parsed = urlparse(target_path)
        bucket = parsed.netloc          # "lakehouse"
        s3_path = bucket + parsed.path  # "lakehouse/bronze/stock_prices/"

        # Auto-create bucket nếu chưa tồn tại (MinIO cần bucket trước khi write)
        if not fs.exists(bucket):
            fs.mkdir(bucket)
            self.logger.info("[Polars] Created S3 bucket: %s", bucket)

        table = df.to_arrow()

        if partition_by:
            import pyarrow as pa
            partition_schema = pa.schema([table.schema.field(c) for c in partition_by])
            pad.write_dataset(
                table,
                base_dir=s3_path,
                filesystem=fs,
                format="parquet",
                partitioning=pad.partitioning(partition_schema, flavor="hive"),
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            # Non-partitioned: single parquet file
            if not s3_path.endswith(".parquet"):
                s3_path = s3_path.rstrip("/") + "/data.parquet"
            with fs.open(s3_path, "wb") as f:
                pq.write_table(table, f)

    def _write_parquet_local(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]],
    ) -> None:
        """Ghi Parquet xuống local filesystem."""
        import pyarrow.dataset as pad
        import pyarrow.parquet as pq
        from pathlib import Path

        table = df.to_arrow()

        if partition_by:
            import pyarrow as pa
            partition_schema = pa.schema([table.schema.field(c) for c in partition_by])
            Path(target_path).mkdir(parents=True, exist_ok=True)
            pad.write_dataset(
                table,
                base_dir=target_path,
                format="parquet",
                partitioning=pad.partitioning(partition_schema, flavor="hive"),
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            local_path = target_path.rstrip("/")
            if not local_path.endswith(".parquet"):
                Path(local_path).mkdir(parents=True, exist_ok=True)
                local_path = local_path + "/data.parquet"
            else:
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, local_path)

    # ------------------------------------------------------------------
    # read_parquet
    # ------------------------------------------------------------------

    def read_parquet(self, path: str) -> Any:
        """Read Parquet from local path or S3/MinIO."""
        import polars as pl

        if self._is_s3_path(path):
            fs = self._build_s3fs()
            if fs is None:
                raise RuntimeError(f"[Polars] Cannot build S3 filesystem for read. path={path}")
            import pyarrow.parquet as pq
            parsed = urlparse(path)
            s3_path = parsed.netloc + parsed.path
            table = pq.read_table(s3_path, filesystem=fs)
            return pl.from_arrow(table)

        return pl.read_parquet(path)
