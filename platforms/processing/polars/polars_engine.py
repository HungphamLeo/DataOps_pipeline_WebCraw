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

    def write_parquet(
        self,
        df: Any,
        target_path: str,
        partition_by: Optional[List[str]] = None,
    ) -> str:
        """Write Polars DataFrame as Parquet to local path or S3."""
        import polars as pl

        if partition_by:
            df.write_parquet(
                target_path,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": partition_by,
                    "existing_data_behavior": "overwrite_or_ignore",
                },
                storage_options=self.config.storage_options or None,
            )
        else:
            df.write_parquet(
                target_path,
                use_pyarrow=True,
                storage_options=self.config.storage_options or None,
            )
        self.logger.debug("[Polars] Wrote %d rows → %s", len(df), target_path)
        return target_path

    def read_parquet(self, path: str) -> Any:
        """Read Parquet from local path or S3."""
        import polars as pl
        return pl.read_parquet(path, storage_options=self.config.storage_options or None)
