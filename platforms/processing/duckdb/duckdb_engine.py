"""
platforms/processing/duckdb/duckdb_engine.py
============================================
Layer: Platform Processing — DuckDB engine wrapper.
Responsibility: Configure DuckDB httpfs (S3/MinIO) and provide query helpers.
Does NOT contain: domain SQL, pipeline business logic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DuckDBConfig:
    database_path: str = ":memory:"
    storage_options: Dict[str, str] = field(default_factory=dict)


class DuckDBEngine:
    """DuckDB engine pre-configured for S3/MinIO httpfs access."""

    def __init__(self, config: DuckDBConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._conn = None
        self._setup()

    def _setup(self) -> None:
        import duckdb
        self._conn = duckdb.connect(self.config.database_path)
        opts = self.config.storage_options
        endpoint = opts.get("endpoint_url", "").replace("http://", "").replace("https://", "").rstrip("/")
        if endpoint:
            try:
                self._conn.execute("INSTALL httpfs; LOAD httpfs;")
                self._conn.execute(f"SET s3_endpoint='{endpoint}';")
                self._conn.execute(f"SET s3_access_key_id='{opts.get('aws_access_key_id', '')}';")
                self._conn.execute(f"SET s3_secret_access_key='{opts.get('aws_secret_access_key', '')}';")
                self._conn.execute("SET s3_use_ssl=false;")
                self._conn.execute("SET s3_url_style='path';")
                self._conn.execute("SET s3_region='us-east-1';")
            except Exception as exc:
                self.logger.warning("[DuckDB] httpfs setup warning: %s", exc)

    @property
    def connection(self):
        return self._conn

    def query_to_polars(self, sql: str) -> Any:
        """Execute SQL and return a Polars LazyFrame (collect() to materialise)."""
        import polars as pl
        try:
            rel = self._conn.execute(sql)
            return pl.from_arrow(rel.arrow()).lazy()
        except Exception as exc:
            self.logger.error("[DuckDB] query failed: %s | sql=%s", exc, sql[:200])
            raise

    def execute(self, sql: str) -> None:
        self._conn.execute(sql)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
