#!/usr/bin/env python3
"""
Unified Master Lakehouse ETL Pipeline
=====================================

Tổng hợp: Master Orchestration + Subsystem Integration + CLI

Kiến trúc: Bronze → Silver → Gold → Serving (PostgreSQL)
Stack: Polars | DuckDB | SQLMesh | Delta Lake | Prefect | PostgreSQL

Usage:
  python scripts/deploy_full_pipeline.py full --symbols FPT VNM --date 2026-05-19
  python scripts/deploy_full_pipeline.py bronze --symbols FPT VNM
  python scripts/deploy_full_pipeline.py silver --date 2026-05-19
  python scripts/deploy_full_pipeline.py gold --date 2026-05-19
  python scripts/deploy_full_pipeline.py serving --date 2026-05-19
  python scripts/deploy_full_pipeline.py validate
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv

# Ensure project root is on sys.path for internal imports.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from shared.logger.python_main_logger import logger_manager

from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import (
    ErrorEventLog,
    ErrorEvent,
    ErrorLevel,
)
from platforms.processing.base_processing_subsystem.subsystem34_metadata_repo import MetadataRepository
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import DataProfiler
from platforms.processing.base_processing_subsystem.subsystem4_data_quality_pre_evaluate import CleansingRuleSet
from platforms.processing.base_processing_subsystem.subsystem7_deduplication import (
    DeduplicationEngine,
    DeduplicationStrategy,
)
from platforms.processing.base_processing_subsystem.subsystem10_surrogate_key_generator import SurrogateKeyGenerator

from platforms.orchestration.prefect.prefect_orchestra_etl import PrefectETLPipelineConfig
from platforms.ingestion.cophieu68.extract.extract_cophieu68 import ExtractCophieu68
from platforms.processing.polars.polars_engine import PolarsConfig, PolarsEngine
from platforms.processing.duckdb.duckdb_engine import DuckDBConfig, DuckDBEngine
from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshConfig, SqlMeshEngine

load_dotenv()

# ==========================================================================
# CONSTANTS & ENV
# ==========================================================================

# LAKEHOUSE_BASE: phải là s3:// (không phải s3a://).
# DuckDB httpfs và s3fs chỉ hỗ trợ s3://, không hỗ trợ s3a:// (Hadoop scheme).
LAKEHOUSE_BASE = os.getenv("LAKEHOUSE_BASE_PATH", "s3://lakehouse")

# S3_ENDPOINT xử lý 2 dạng:
#   - DuckDB httpfs cần HOST:PORT (không có http://)
#   - s3fs/boto3 cần URL đầy đủ http://HOST:PORT
_S3_ENDPOINT_RAW = os.getenv("S3_ENDPOINT", "http://localhost:9000")
# Strip scheme cho DuckDB — "http://localhost:9000" → "localhost:9000"
S3_ENDPOINT = _S3_ENDPOINT_RAW.split("://")[-1]
# Giữ full URL cho s3fs (cần http:// prefix)
S3_ENDPOINT_URL = _S3_ENDPOINT_RAW if "://" in _S3_ENDPOINT_RAW else f"http://{_S3_ENDPOINT_RAW}"

# BUG-A FIX: SQLMesh đọc S3_ENDPOINT từ os.environ khi khởi tạo Context.
# Phải ghi đè env var S3_ENDPOINT thành HOST:PORT (đã stripped) TRƯỚC khi
# SQLMesh Context được khởi tạo — tránh lỗi "//localhost:9000" trong DuckDB httpfs.
os.environ["S3_ENDPOINT"] = S3_ENDPOINT

# Credentials: đọc AWS_* trước (boto3/s3fs convention), fallback MINIO_* (docker-compose convention)
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_123@#"))
# Đồng bộ credentials vào os.environ để SQLMesh pre_statements nhận đúng
os.environ.setdefault("AWS_ACCESS_KEY_ID", S3_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", S3_SECRET)

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "etl_project")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

SQLMESH_PATH = os.getenv("SQLMESH_PATH", str(PROJECT_ROOT / "platforms" / "processing" / "sqlmesh"))
SQLMESH_GATEWAY = os.getenv("SQLMESH_GATEWAY", "local_duckdb")

DEFAULT_SYMBOLS = ["FPT", "VNM", "HPG", "MBB", "SSI"]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "platforms" / "orchestration" / "prefect" / "config" / "cophieu68_config.yaml"

# STORAGE_OPTIONS dùng cho s3fs/boto3 (Polars write_parquet) — cần URL đầy đủ http://HOST:PORT
STORAGE_OPTIONS: Dict[str, str] = {
    "endpoint_url": S3_ENDPOINT_URL,
    "aws_access_key_id": S3_KEY,
    "aws_secret_access_key": S3_SECRET,
}

# ==========================================================================
# HELPERS: IDs + config builders
# ==========================================================================

def make_batch_id(symbol: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"batch_{symbol.upper()}_{ts}_{uid}"


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"run_{ts}_{uid}"


class ExecutionPhase(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"
    FULL = "full"
    VALIDATE = "validate"


class ProcessingBackend(str, Enum):
    POLARS = "polars"
    SPARK = "spark"
    SQLMESH = "sqlmesh"
    DUCKDB = "duckdb"
    DBT = "dbt"


@dataclass
class ExecutionContext:
    run_id: str
    phase: ExecutionPhase
    symbols: List[str]
    backend: ProcessingBackend
    target_date: str
    environment: str = "dev"
    dry_run: bool = False
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    status: str = "RUNNING"
    error_message: Optional[str] = None
    metadata_repo: Optional[MetadataRepository] = None
    error_log: Optional[ErrorEventLog] = None
    logger: Optional[logging.Logger] = None

    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()


class ConfigurationManager:
    def __init__(self, config_path: Optional[str] = None, logger: Optional[logging.Logger] = None):
        self.logger = logger or logger_manager.get_logger(__name__)
        self.config_path = Path(config_path or DEFAULT_CONFIG_PATH)
        self.config: Dict[str, Any] = {}
        self.prefect_config: Optional[PrefectETLPipelineConfig] = None

    def load(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f) or {}
            self.logger.info(f"[CONFIG] Loaded from {self.config_path}")
            return self.config
        except FileNotFoundError:
            self.logger.error(f"[CONFIG] File not found: {self.config_path}")
            return {}
        except Exception as exc:
            self.logger.error(f"[CONFIG] Error loading config: {exc}")
            return {}

    def load_prefect_config(self) -> Optional[PrefectETLPipelineConfig]:
        try:
            self.prefect_config = PrefectETLPipelineConfig(config_path=str(self.config_path))
            self.logger.info("[CONFIG] Prefect config loaded successfully")
            return self.prefect_config
        except Exception as exc:
            self.logger.error(f"[CONFIG] Error loading Prefect config: {exc}")
            return None

    def validate(self) -> Dict[str, Any]:
        errors = []
        if not self.config:
            errors.append("Configuration is empty")
        # Config is raw YAML: required sections live under project_params
        params = self.config.get("project_params", self.config)
        required_sections = ["sources", "http"]
        for section in required_sections:
            if section not in params:
                errors.append(f"Missing required section in project_params: {section}")
        if not params.get("sources", {}).get("cophieu68", {}).get("base_url"):
            errors.append("Missing project_params.sources.cophieu68.base_url")
        return {"is_valid": not errors, "errors": errors}


def _params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap project_params wrapper if present (raw YAML vs pre-unwrapped dict)."""
    return config.get("project_params", config)


def _build_polars_engine(config: Dict[str, Any]) -> PolarsEngine:
    params = _params(config)
    cfg = PolarsConfig(
        thread_pool_size=params.get("polars", {}).get("thread_pool_size"),
        enable_streaming=params.get("polars", {}).get("enable_streaming", True),
        storage_options=STORAGE_OPTIONS,
    )
    return PolarsEngine(config=cfg, logger=logger_manager.get_logger("polars_engine"))


def _build_duckdb_engine(config: Optional[Dict[str, Any]] = None) -> DuckDBEngine:
    duck_cfg = DuckDBConfig(
        database_path=":memory:",
        storage_options=STORAGE_OPTIONS,
    )
    return DuckDBEngine(config=duck_cfg, logger=logger_manager.get_logger("duckdb_engine"))


def _build_sqlmesh_engine(config: Optional[Dict[str, Any]] = None) -> SqlMeshEngine:
    cfg = SqlMeshConfig(
        project_path=SQLMESH_PATH,
        gateway=SQLMESH_GATEWAY,
    )
    return SqlMeshEngine(config=cfg, logger=logger_manager.get_logger("sqlmesh_engine"))


def _build_extractor(config: Dict[str, Any]) -> ExtractCophieu68:
    params = _params(config)
    cophieu_cfg = params.get("sources", {}).get("cophieu68", {})
    pipeline_cfg = {
        "sources": {"cophieu68": cophieu_cfg},
        "http": params.get("http", {"delay_seconds": 0.5, "timeout_seconds": 30}),
    }
    return ExtractCophieu68(pipeline_config=pipeline_cfg, pipeline_logger=logger_manager.get_logger("extractor"))


def _build_cleansing_rules(symbol: str) -> CleansingRuleSet:
    ruleset = CleansingRuleSet(table_name="stock_prices")

    def symbol_not_null(rec: Dict[str, Any]):
        val = rec.get("symbol")
        if not val:
            return False, f"[DQ] symbol is null for record: {rec}"
        return True, ""

    def positive_close(rec: Dict[str, Any]):
        try:
            close = float(rec.get("close_price") or rec.get("close") or 0)
            if close <= 0:
                return False, f"[DQ] close_price <= 0: {close}"
        except (TypeError, ValueError):
            return False, f"[DQ] close_price không parse được: {rec.get('close_price')}"
        return True, ""

    def non_negative_volume(rec: Dict[str, Any]):
        try:
            vol = float(rec.get("volume") or 0)
            if vol < 0:
                return False, f"[DQ] volume < 0: {vol}"
        except (TypeError, ValueError):
            pass
        return True, ""

    ruleset.add_rule(symbol_not_null)
    ruleset.add_rule(positive_close)
    ruleset.add_rule(non_negative_volume)
    return ruleset


class BronzePolarsIngester:
    def __init__(self, engine: PolarsEngine, table_name: str = "stock_prices", base_path: str = LAKEHOUSE_BASE):
        self.engine = engine
        self.table_name = table_name
        self.base_path = base_path
        self.logger = engine.logger
        self.governance_logger = logger_manager.get_logger("logger.governance.data_quality")

    def _profile(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {"total_rows": 0, "columns": []}
        sample = records[0]
        cols = list(sample.keys())
        total = len(records)
        null_counts = {col: sum(1 for r in records if r.get(col) is None) for col in cols}
        return {"total_rows": total, "columns": cols, "null_counts": null_counts}

    def _evaluate_dq(
        self,
        records: List[Dict[str, Any]],
        cleansing: CleansingRuleSet,
        run_id: str,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        processed: List[Dict[str, Any]] = []
        dq_violations: List[Dict[str, Any]] = []
        for rec in records:
            valid, messages = cleansing.apply(rec)
            rec["_dq_status"] = "PASS" if valid else "WARN"
            rec["_dq_errors"] = "; ".join(messages) if messages else ""
            rec["_run_id"] = run_id
            if not valid:
                dq_violations.append({"record": rec, "messages": messages})
            processed.append(rec)
        return processed, dq_violations

    def process(
        self,
        raw_records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: str,
        cleansing: Optional[CleansingRuleSet] = None,
    ) -> Dict[str, Any]:
        import polars as pl

        self.logger.info(f"[BronzeIngester:{symbol}] Start processing {len(raw_records)} records.")
        profile = self._profile(raw_records)

        if cleansing:
            enriched_records, dq_violations = self._evaluate_dq(raw_records, cleansing, run_id)
        else:
            enriched_records = raw_records
            dq_violations = []

        if dq_violations:
            self.logger.warning(f"[BronzeIngester:{symbol}] {len(dq_violations)} data quality issues detected, records will still be ingested.")
            for idx, violation in enumerate(dq_violations, start=1):
                detail = violation["record"]
                message = violation["messages"]
                log_msg = f"[DQ][{symbol}] violation {idx}/{len(dq_violations)}: {'; '.join(message)} | record={detail}"
                self.governance_logger.warning(log_msg)

        if not enriched_records:
            self.logger.warning(f"[BronzeIngester:{symbol}] No records to write after evaluation.")
            return {
                "saved_path": None,
                "reject_path": None,
                "stats": {**profile, "clean": 0, "dq_violations": len(dq_violations)},
            }

        df = pl.DataFrame(enriched_records)
        if "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

        df = df.with_columns([
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(run_id).alias("_run_id"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingest_timestamp"),
            pl.lit(datetime.now(timezone.utc).date().isoformat()).alias("ingest_date"),
        ])

        bronze_path = f"{self.base_path}/bronze/{self.table_name}/"
        saved_path = self.engine.write_parquet(df=df, target_path=bronze_path, partition_by=["ingest_date"])

        reject_path = None
        if dq_violations:
            reject_path = f"{self.base_path}/bronze/_dq_violations/{self.table_name}/"
            reject_df = pl.DataFrame([violation["record"] for violation in dq_violations])
            self.engine.write_parquet(df=reject_df, target_path=reject_path)

        return {
            "saved_path": saved_path,
            "reject_path": reject_path,
            "stats": {**profile, "clean": len(enriched_records), "dq_violations": len(dq_violations)},
        }

    def ingest(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: Optional[str] = None,
        cleansing: Optional[CleansingRuleSet] = None,
    ) -> Dict[str, Any]:
        """Generic ingestion helper: normalize → DQ evaluate → write Parquet.

        Dùng cho mọi bảng Bronze ngoài stock_prices (company_profile,
        financial_ratios, income_statement, v.v.)  Trả về stats dict
        chuẩn để BronzeExecutor log lineage.
        """
        import polars as pl

        if not records:
            self.logger.warning(f"[BronzeIngester] No records for table={table_name} symbol={symbol}")
            return {"saved_path": None, "rows": 0, "dq_violations": 0}

        # Normalise records:
        #   1. Flatten nested pd.DataFrame → JSON string
        #   2. Cast EVERY value to str (Bronze = raw as-is, avoid Polars
        #      schema-mismatch when pd.read_html returns mixed-type columns)
        #
        # BUG-B FIX: schema_overrides={c: pl.Utf8 for c in flat[0].keys()} bị lỗi
        # "'int' object cannot be converted to 'PyString'" khi record có giá trị int
        # bị nhầm là column name. Nguyên nhân thực: flat[0] sau _evaluate_dq có thể
        # có key là string nhưng value là int (vd: _dq_status index).
        # Giải pháp: cast toàn bộ values sang str TRƯỚC khi tạo Polars DataFrame,
        # KHÔNG dùng schema_overrides (Polars tự infer từ data sau khi đã str).
        import pandas as _pd
        import numpy as _np

        flat: List[Dict[str, Any]] = []
        for rec in records:
            flat_rec: Dict[str, Any] = {}
            for k, v in rec.items():
                if not isinstance(k, str):
                    # Bỏ qua key không phải string (edge case với dict từ pandas)
                    continue
                if isinstance(v, _pd.DataFrame):
                    flat_rec[k] = v.to_json(orient="records")
                elif v is None:
                    flat_rec[k] = None
                elif isinstance(v, (_np.integer, _np.floating)):
                    # numpy scalar → convert sang Python native trước khi str()
                    flat_rec[k] = str(v.item())
                elif isinstance(v, (_np.ndarray,)):
                    flat_rec[k] = str(v.tolist())
                else:
                    flat_rec[k] = str(v)
            flat.append(flat_rec)

        if cleansing:
            flat, dq_violations = self._evaluate_dq(flat, cleansing, run_id)
        else:
            dq_violations = []

        if not flat:
            self.logger.warning(f"[BronzeIngester] All records empty after normalisation for table={table_name}")
            return {"saved_path": None, "rows": 0, "dq_violations": 0}

        # Đảm bảo tất cả values trong flat là str hoặc None (Polars Utf8 compatible)
        # schema_overrides chỉ dùng keys từ flat[0] — keys luôn là str sau normalize
        str_cols = {c: pl.Utf8 for c in flat[0].keys() if isinstance(c, str)}
        df = pl.DataFrame(flat, schema_overrides=str_cols)

        if symbol and "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

        df = df.with_columns([
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(run_id).alias("_run_id"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingest_timestamp"),
            pl.lit(datetime.now(timezone.utc).date().isoformat()).alias("ingest_date"),
        ])

        path = f"{self.base_path}/bronze/{table_name}/"
        self.engine.write_parquet(df=df, target_path=path, partition_by=["ingest_date"])
        self.logger.info(f"[BronzeIngester] Wrote {len(df)} rows → {path}")

        return {"saved_path": path, "rows": len(df), "dq_violations": len(dq_violations)}


class SilverProcessor:
    def __init__(
        self,
        duckdb_engine: DuckDBEngine,
        polars_engine: PolarsEngine,
        base_path: str = LAKEHOUSE_BASE,
    ) -> None:
        self.duck = duckdb_engine
        self.polars = polars_engine
        self.sk_gen = SurrogateKeyGenerator(prefix="STK_", key_length=32)
        self.base = base_path
        self.logger = polars_engine.logger

    def transform_stock_prices(
        self,
        target_date: str,
        run_id: str,
        dedup_strategy: DeduplicationStrategy = DeduplicationStrategy.KEEP_LAST,
    ) -> Dict[str, Any]:
        """bronze/stock_prices → silver/fact_stock_price  (SILVER_FACT_TRADING_HISTORY)

        Schema: trade_key (SK), symbol, trade_date, close_price, open_price,
                high_price, low_price, volume, foreign_buy, foreign_sell,
                foreign_net_value, year, month,
                _ingested_at, _pipeline_run_id
        Partition: [year, month]
        """
        import polars as pl

        # Re-use _read_bronze so ingest_date is materialised via hive_partitioning=true
        df = self._read_bronze("stock_prices", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        rows_in = len(df)
        self.logger.info(f"[SilverProcessor] Read {rows_in} rows from bronze.stock_prices")

        df = self._dedup(df, keys=["symbol", "date"], run_id=run_id,
                         source="bronze.stock_prices")
        dedup_stats = {}

        # Surrogate key: SHA256(symbol+date) — matches schema registry "trade_key"
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("date")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("trade_key")
        )

        # Add partition columns year/month from ingest_date (re-materialised by hive_partitioning)
        if "ingest_date" in df.columns:
            df = df.with_columns([
                pl.col("ingest_date").str.slice(0, 4).cast(pl.Int32).alias("year"),
                pl.col("ingest_date").str.slice(5, 2).cast(pl.Int32).alias("month"),
            ])

        df = self._add_audit(df, run_id)

        # Partition by [year, month] per schema registry
        partition_cols = (["year", "month"]
                          if "year" in df.columns and "month" in df.columns else [])
        silver_path = self._write_silver(df, "fact_stock_price",
                                         partition_by=partition_cols)
        rows_out = len(df)

        return {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "dedup_stats": dedup_stats,
            "silver_path": silver_path,
        }

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _read_bronze(self, table_name: str, target_date: str) -> Optional[Any]:
        """Read bronze/<table_name>/ via DuckDB.

        Strategy (in order):
          1. Try exact partition: ingest_date=<target_date>/*.parquet
          2. Fallback: scan all partitions via /**/*.parquet (latest wins at dedup step)

        Uses hive_partitioning=true so `ingest_date` is re-materialised as a column.
        Returns a Polars DataFrame, or None if table is entirely empty/missing.

        BUG-C FIX: Bronze Parquet có thể có cột datetime[ms] (do PyArrow auto-infer
        khi ghi file hoặc do pandas DataFrame column dtype). Silver transforms dùng
        .str.slice(), .str.contains() trên các cột này → lỗi "expected String, got
        datetime[ms]". Sau khi đọc, cast TẤT CẢ cột không phải số sang Utf8.
        """
        import polars as pl

        def _query(glob: str) -> Optional[Any]:
            try:
                df = self.duck.query_to_polars(
                    f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
                ).collect()
                return df if not df.is_empty() else None
            except Exception as exc:
                self.logger.debug(f"[SilverProcessor] read_parquet({glob}) failed: {exc}")
                return None

        def _normalise_dtypes(df: Any) -> Any:
            """Cast tất cả cột datetime/date/time về Utf8 (ISO string).

            Bronze layer lưu raw data dưới dạng string, nhưng PyArrow/DuckDB
            đôi khi infer datetime khi đọc lại. Silver transforms mong đợi Utf8
            cho tất cả cột (trừ numeric columns sẽ được cast riêng sau).
            Các cột numeric (Int*, Float*, UInt*) giữ nguyên — không cast sang str
            để tránh mất thông tin precision trong dedup/sort.
            """
            if df is None:
                return None
            cast_exprs = []
            KEEP_NUMERIC = (
                pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                pl.Float32, pl.Float64,
            )
            for col_name, dtype in zip(df.columns, df.dtypes):
                if isinstance(dtype, KEEP_NUMERIC):
                    continue  # Giữ nguyên numeric
                if dtype == pl.Utf8 or dtype == pl.String:
                    continue  # Đã là string
                # datetime, date, time, bool, categorical, ... → cast sang Utf8
                cast_exprs.append(
                    pl.col(col_name).cast(pl.Utf8, strict=False).alias(col_name)
                )
            if cast_exprs:
                df = df.with_columns(cast_exprs)
            return df

        # 1. Exact date partition
        exact_glob = f"{self.base}/bronze/{table_name}/ingest_date={target_date}/*.parquet"
        self.logger.info(f"[SilverProcessor] Reading bronze/{table_name} (exact: {target_date})")
        df = _query(exact_glob)
        if df is not None:
            return _normalise_dtypes(df)

        # 2. Fallback: scan all partitions — picks up data from any ingest date
        all_glob = f"{self.base}/bronze/{table_name}/**/*.parquet"
        self.logger.warning(
            f"[SilverProcessor] No data for ingest_date={target_date} in "
            f"bronze/{table_name} — falling back to full scan {all_glob}"
        )
        df = _query(all_glob)
        if df is not None:
            self.logger.info(
                f"[SilverProcessor] Fallback scan found {len(df)} rows in bronze/{table_name}")
            return _normalise_dtypes(df)

        self.logger.warning(f"[SilverProcessor] bronze/{table_name} is empty — skipping")
        return None

    def _write_silver(self, df: Any, silver_table: str,
                      partition_by: Optional[List[str]] = None) -> str:
        """Write a Polars DataFrame to silver/<silver_table>/ and return the path.

        partition_by=[] → PyArrow raises ValueError("Must pass at least one partition column").
        Convert empty list to None so _write_parquet_s3 / write_parquet takes the
        non-partitioned code path (single data.parquet file).
        """
        silver_path = f"{self.base}/silver/{silver_table}/"
        # Normalise: [] và None đều → None (non-partitioned write)
        _parts = partition_by if partition_by else None
        self.polars.write_parquet(df=df, target_path=silver_path,
                                  partition_by=_parts)
        self.logger.info(f"[SilverProcessor] Wrote {len(df)} rows → {silver_path}")
        return silver_path

    def _add_audit(self, df: Any, run_id: str) -> Any:
        """Add _ingested_at and _pipeline_run_id audit columns (matches schema registry)."""
        import polars as pl
        now_ts = datetime.now(timezone.utc).isoformat()
        return df.with_columns([
            pl.lit(now_ts).alias("_ingested_at"),
            pl.lit(run_id).alias("_pipeline_run_id"),
        ])

    def _dedup(self, df: Any, keys: List[str], run_id: str, source: str) -> Any:
        """Deduplicate a Polars DataFrame via DeduplicationEngine (pandas bridge).
        Falls back to KEEP_LAST if any key column is absent."""
        import polars as pl
        df_pd = df.to_pandas()
        # Only use keys that actually exist in the dataframe
        valid_keys = [k for k in keys if k in df_pd.columns]
        if not valid_keys:
            self.logger.warning(
                f"[SilverProcessor] Dedup({source}): none of keys {keys} found, skipping")
            return df
        engine = DeduplicationEngine(
            keys=valid_keys,
            strategy=DeduplicationStrategy.KEEP_LAST,
            tiebreaker_col="_ingest_timestamp",
        )
        deduped_pd, stats = engine.deduplicate_dataframe(
            df=df_pd, source=source, run_id=run_id)
        self.logger.info(f"[SilverProcessor] Dedup({source}) stats: {stats}")
        return pl.from_pandas(deduped_pd)

    def _safe_cast(self, df: Any, col: str, dtype: Any) -> Any:
        """Cast a column to dtype, replacing errors with null. No-op if col absent."""
        import polars as pl
        if col not in df.columns:
            return df
        return df.with_columns(
            pl.col(col).cast(dtype, strict=False).alias(col)
        )

    # ------------------------------------------------------------------
    # Per-symbol transforms — aligned with delta_schema_registry.py
    # ------------------------------------------------------------------

    def transform_company_profile(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/company_profile → silver/dim_company  (SILVER_DIM_COMPANY)

        Schema: company_key (SK), symbol, full_name, english_name, short_name,
                address, phone, fax, website, email_address, established_date,
                listed_date, listed_volume_initial, listed_volume,
                circulating_volume, market_capitalization,
                effective_date, end_date, is_current, _row_hash,
                _ingested_at, _pipeline_run_id
        """
        import polars as pl
        df = self._read_bronze("company_profile", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        df = self._dedup(df, keys=["symbol"], run_id=run_id,
                         source="bronze.company_profile")

        # Build surrogate key: SHA256(symbol) — matches schema registry "company_key"
        df = df.with_columns(
            pl.col("symbol")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("company_key")
        )

        # SCD2 columns — for now open-ended (no expiry)
        today = datetime.now(timezone.utc).date().isoformat()
        df = df.with_columns([
            pl.lit(today).alias("effective_date"),
            pl.lit(None).cast(pl.Utf8).alias("end_date"),
            pl.lit(True).alias("is_current"),
        ])

        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "dim_company", partition_by=[])
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_financial_ratios(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/financial_ratios → silver/fact_financial_metrics  (SILVER_FACT_FINANCIAL_METRICS)

        Schema: financial_ratio_key (SK), symbol, reference_price, open_price,
                high_price, low_price, volume, book_value, eps, pe, pb, roe, roa,
                beta, market_cap, listed_volume, avg_volume_52w, high_low_52w,
                debt, equity, debt_to_equity, equity_to_assets, cash,
                update_time, _ingested_at, _pipeline_run_id
        """
        import polars as pl
        df = self._read_bronze("financial_ratios", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        df = self._dedup(df, keys=["symbol"], run_id=run_id,
                         source="bronze.financial_ratios")

        # Surrogate key: SHA256(symbol + ingest_date)
        ingest_date_val = target_date
        df = df.with_columns(
            pl.concat_str(
                [pl.col("symbol"), pl.lit(ingest_date_val)], separator="|"
            ).map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("financial_ratio_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )

        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "fact_financial_metrics", partition_by=[])
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_financial_report(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/financial_report_summary → silver/fact_financial_report

        No dedicated schema registry entry yet — store as-is with audit cols.
        Partition by ingest_date (column is re-materialised by hive_partitioning=true).
        """
        import polars as pl
        df = self._read_bronze("financial_report_summary", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        dedup_keys = [k for k in ["symbol", "report_type"] if k in df.columns]
        df = self._dedup(df, keys=dedup_keys, run_id=run_id,
                         source="bronze.financial_report_summary")
        df = self._add_audit(df, run_id)
        # ingest_date exists because hive_partitioning=true re-adds it
        partition_cols = ["ingest_date"] if "ingest_date" in df.columns else []
        silver_path = self._write_silver(df, "fact_financial_report",
                                         partition_by=partition_cols)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_business_plan(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/business_plan → silver/fact_business_plan  (SILVER_FACT_BUSINESS_PLAN)

        Schema: plan_key (SK), symbol, year, plan_revenue, pass_revenue,
                plan_profit, pass_profit, update_time,
                _ingested_at, _pipeline_run_id
        """
        import polars as pl
        df = self._read_bronze("business_plan", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        # Normalise Year column name (Bronze stores it as "Year" capitalised)
        if "Year" in df.columns and "year" not in df.columns:
            df = df.rename({"Year": "year"})

        df = self._dedup(df, keys=["symbol", "year"], run_id=run_id,
                         source="bronze.business_plan")

        # Surrogate key: SHA256(symbol+year)
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("year")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("plan_key")
        )

        # Cast numeric columns
        for col_name in ("plan_revenue", "pass_revenue", "plan_profit", "pass_profit",
                         "Plan_revenue", "Pass_revenue", "Plan_profit", "Pass_profit"):
            df = self._safe_cast(df, col_name, pl.Float64)

        # Normalise column names to lowercase
        df = df.rename({c: c.lower() for c in df.columns})

        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "fact_business_plan", partition_by=[])
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_income_statement(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/income_statement_{quarter,year} → silver/fact_income_statement
           (SILVER_FACT_INCOME_STATEMENT)

        Schema: income_key (SK), symbol, time_report_type (ANNUALLY|QUARTERLY),
                financial_report_type, year, period, metric_code, metric_name_en,
                metric_group, metric_value, currency, unit, update_time,
                _ingested_at, _pipeline_run_id
        Partition: [time_report_type, year]

        Bronze stores the whole DataFrame as a JSON blob; we keep it as-is here
        (full normalization to per-metric rows is a Gold/SQLMesh concern).
        """
        import polars as pl
        frames = []
        for report_type, time_report_type in (("quarter", "QUARTERLY"), ("year", "ANNUALLY")):
            df = self._read_bronze(f"income_statement_{report_type}", target_date)
            if df is not None:
                df = df.with_columns([
                    pl.lit(time_report_type).alias("time_report_type"),
                    pl.lit(report_type).alias("report_type"),
                ])
                frames.append(df)
        if not frames:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        df = pl.concat(frames, how="diagonal")
        rows_in = len(df)

        df = self._dedup(df, keys=["symbol", "time_report_type"], run_id=run_id,
                         source="bronze.income_statement")

        # Surrogate key
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("time_report_type")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("income_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)

        # Extract year from ingest_date for partitioning (YYYY from YYYY-MM-DD)
        if "ingest_date" in df.columns:
            df = df.with_columns(
                pl.col("ingest_date").str.slice(0, 4).alias("year")
            )
        partition_cols = (["time_report_type", "year"]
                          if "time_report_type" in df.columns and "year" in df.columns
                          else [])
        silver_path = self._write_silver(df, "fact_income_statement",
                                         partition_by=partition_cols)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_balance_sheet(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/balance_sheet_{quarter,year} → silver/fact_balance_sheet
           (SILVER_FACT_BALANCE_SHEET)

        Same pattern as income_statement.
        Partition: [time_report_type, year]
        """
        import polars as pl
        frames = []
        for report_type, time_report_type in (("quarter", "QUARTERLY"), ("year", "ANNUALLY")):
            df = self._read_bronze(f"balance_sheet_{report_type}", target_date)
            if df is not None:
                df = df.with_columns([
                    pl.lit(time_report_type).alias("time_report_type"),
                    pl.lit(report_type).alias("report_type"),
                ])
                frames.append(df)
        if not frames:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        df = pl.concat(frames, how="diagonal")
        rows_in = len(df)

        df = self._dedup(df, keys=["symbol", "time_report_type"], run_id=run_id,
                         source="bronze.balance_sheet")

        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("time_report_type")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("balance_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)

        if "ingest_date" in df.columns:
            df = df.with_columns(
                pl.col("ingest_date").str.slice(0, 4).alias("year")
            )
        partition_cols = (["time_report_type", "year"]
                          if "time_report_type" in df.columns and "year" in df.columns
                          else [])
        silver_path = self._write_silver(df, "fact_balance_sheet",
                                         partition_by=partition_cols)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    # ------------------------------------------------------------------
    # Global transforms — aligned with delta_schema_registry.py
    # ------------------------------------------------------------------

    def transform_industry_sectors(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/industry_sectors → silver/dim_industry  (SILVER_DIM_INDUSTRY)

        Schema: industry_sk (SK), industry_code, industry_name, industry_metric,
                industry_craw_url, effective_date, end_date, is_current,
                _row_hash, _ingested_at, _pipeline_run_id
        """
        import polars as pl
        df = self._read_bronze("industry_sectors", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        df = self._dedup(df, keys=["industry_code", "symbol"], run_id=run_id,
                         source="bronze.industry_sectors")

        # Surrogate key: SHA256(industry_code)
        df = df.with_columns(
            pl.col("industry_code")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("industry_sk")
        )

        today = datetime.now(timezone.utc).date().isoformat()
        df = df.with_columns([
            pl.lit(today).alias("effective_date"),
            pl.lit(None).cast(pl.Utf8).alias("end_date"),
            pl.lit(True).alias("is_current"),
        ])

        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "dim_industry", partition_by=[])
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_market_type_sectors(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/market_type_sectors → silver/dim_market_type  (SILVER_DIM_MARKET_TYPE)

        Schema: market_key (SK), market_type, market_name, description,
                update_time, _ingested_at
        """
        import polars as pl
        df = self._read_bronze("market_type_sectors", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        df = self._dedup(df, keys=["market_type_code"], run_id=run_id,
                         source="bronze.market_type_sectors")

        # Rename market_type_code → market_type (schema registry uses market_type)
        if "market_type_code" in df.columns and "market_type" not in df.columns:
            df = df.rename({"market_type_code": "market_type"})
        if "market_type_name" in df.columns and "market_name" not in df.columns:
            df = df.rename({"market_type_name": "market_name"})

        # Surrogate key: SHA256(market_type)
        df = df.with_columns(
            pl.col("market_type")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("market_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "dim_market_type", partition_by=[])
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_industry_info(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/industry_info_{summary,financial,fund} → silver/fact_industry_summary
           (SILVER_FACT_INDUSTRY_SUMMARY)

        Schema: industry_summary_key (SK), industry_code, industry_name,
                industry_metric_type, industry_index, percentage_change, liquidity,
                total_capital, average_price, book_value, eps, pe, roa, roe,
                supply_volumn, total_asset, total_equity, total_liabilities,
                percentage_debt_on_equity, percentage_equity_on_assets,
                revenue, profit_before_tax, update_time,
                _ingested_at, _pipeline_run_id
        Partition: [] (no partition per schema registry)
        """
        import polars as pl
        frames = []
        for type_info in ("summary_info", "financial_info", "fund_info"):
            df = self._read_bronze(f"industry_info_{type_info}", target_date)
            if df is not None:
                df = df.with_columns(
                    pl.lit(type_info).alias("industry_metric_type")
                )
                frames.append(df)
        if not frames:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        df = pl.concat(frames, how="diagonal")
        rows_in = len(df)

        # Dedup on natural key: (_industry_key, industry_metric_type)
        dedup_keys = [k for k in ["_industry_key", "industry_metric_type"]
                      if k in df.columns]
        df = self._dedup(df, keys=dedup_keys, run_id=run_id,
                         source="bronze.industry_info")

        # Surrogate key: SHA256(_industry_key + industry_metric_type)
        key_cols = [c for c in ["_industry_key", "industry_metric_type"] if c in df.columns]
        if key_cols:
            df = df.with_columns(
                pl.concat_str([pl.col(c) for c in key_cols], separator="|")
                .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
                .alias("industry_summary_key")
            )

        # Cast numeric metric columns (all arrive as Utf8 from Bronze)
        for col_name in ("pe", "roa", "roe", "industry_index", "percentage_change",
                         "liquidity", "total_capital", "supply_volumn", "total_asset",
                         "total_equity", "total_liabilities",
                         "percentage_debt_on_equity", "percentage_equity_on_assets",
                         "revenue", "profit_before_tax"):
            df = self._safe_cast(df, col_name, pl.Float64)

        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        # No partition per SILVER_FACT_INDUSTRY_SUMMARY schema registry
        silver_path = self._write_silver(df, "fact_industry_summary", partition_by=[])
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}


class GoldProcessor:
    def __init__(self, sqlmesh_engine: SqlMeshEngine, duck_engine: DuckDBEngine) -> None:
        self.sqlmesh = sqlmesh_engine
        self.duck = duck_engine
        self.logger = sqlmesh_engine.logger

    def run_gold_models(
        self,
        environment: str = "prod",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        run_audits: bool = True,
    ) -> Dict[str, Any]:
        self.logger.info(
            f"[GoldProcessor] Running SQLMesh models env={environment} "
            f"start={start_date} end={end_date}"
        )
        result: Dict[str, Any] = {"environment": environment}

        # ── Step 1: plan + backfill ──────────────────────────────────────────
        # SQLMesh FULL models execute SELECT ... FROM read_parquet(s3://...) on
        # every plan.  When MinIO is offline or silver data doesn't exist yet,
        # this always fails.  We catch any IOException / PlanError and treat
        # the gold phase as SKIPPED (not FAILED) so the overall pipeline
        # can continue.  Gold will populate correctly on the next run once
        # bronze → silver data exists on MinIO.
        try:
            plan_result = self.sqlmesh.plan(environment=environment)
            result["plan"] = "applied"
            self.logger.info("[GoldProcessor] SQLMesh plan+backfill complete")
        except Exception as exc:
            exc_msg = str(exc)
            # S3/MinIO connection errors are expected when silver isn't ready yet
            if any(kw in exc_msg for kw in ("Connection error", "IO Error", "Plan application")):
                self.logger.warning(
                    f"[GoldProcessor] Gold skipped — silver data not yet available "
                    f"on MinIO or MinIO offline ({exc_msg[:120]}). "
                    "Run bronze+silver first, then gold."
                )
                result["plan"] = "skipped_no_silver_data"
                result["run_status"] = "SKIPPED"
                return result
            # Unexpected error — re-raise so GoldExecutor catches it properly
            raise

        # ── Step 2: run incremental ──────────────────────────────────────────
        try:
            self.sqlmesh.run(environment=environment, start=start_date, end=end_date)
            result["run_status"] = "SUCCESS"
            self.logger.info("[GoldProcessor] SQLMesh run complete")
        except Exception as exc:
            self.logger.warning(f"[GoldProcessor] SQLMesh run warning: {exc}")
            result["run_status"] = f"WARN: {exc}"

        # ── Step 3: audit ────────────────────────────────────────────────────
        if run_audits:
            try:
                self.sqlmesh.audit()
                result["audits"] = "PASSED"
                self.logger.info("[GoldProcessor] SQLMesh audits complete")
            except Exception as exc:
                self.logger.warning(f"[GoldProcessor] Audit warning: {exc}")
                result["audits"] = f"WARN: {exc}"

        return result


class ServingSyncProcessor:
    def __init__(self, duck_engine: DuckDBEngine, base_path: str = LAKEHOUSE_BASE):
        self.duck = duck_engine
        self.base = base_path
        self.logger = duck_engine.logger

    def _pg_conn_string(self) -> str:
        return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

    def sync_gold_to_postgres(self, mart_table: str = "mart_kpi_daily", target_date: Optional[str] = None) -> Dict[str, Any]:
        if not PG_USER:
            self.logger.warning("[ServingSync] PostgreSQL credentials not configured")
            return {"skipped": True, "reason": "missing_pg_credentials"}

        gold_glob = f"{self.base}/gold/{mart_table}/"
        if target_date:
            gold_glob += f"date={target_date}/*.parquet"
        else:
            gold_glob += "**/*.parquet"

        self.logger.info(f"[ServingSync] Syncing {mart_table} from {gold_glob} to PostgreSQL")
        con = self.duck.connection
        con.execute("INSTALL postgres; LOAD postgres;")
        pg_conn = self._pg_conn_string()
        con.execute(f"ATTACH '{pg_conn}' AS pg_serving (TYPE POSTGRES, READ_WRITE);")
        con.execute(
            f"""
            INSERT INTO pg_serving.gold.{mart_table}
                SELECT * FROM read_parquet('{gold_glob}', hive_partitioning=true)
            ON CONFLICT DO NOTHING;
            """
        )

        return {"status": "SUCCESS", "mart_table": mart_table}

    def optimize_gold_tables(self) -> None:
        self.logger.info("[ServingSync] OPTIMIZE placeholder called")


class BronzeExecutor:
    """Orchestrates full Bronze ingestion for all crawl_* methods.

    Per-symbol tables (run for each symbol in context.symbols):
        bronze/stock_prices          ← crawl_trading_data
        bronze/company_profile       ← crawl_company_profile
        bronze/financial_ratios      ← crawl_financial_ratios
        bronze/financial_report_summary ← crawl_financial_report_summary
        bronze/business_plan         ← crawl_business_plan
        bronze/income_statement_quarter ← crawl_details_income_statement (quarter)
        bronze/income_statement_year    ← crawl_details_income_statement (year)
        bronze/balance_sheet_quarter    ← crawl_details_balance_sheet (quarter)
        bronze/balance_sheet_year       ← crawl_details_balance_sheet (year)

    Global tables (run once, not per-symbol):
        bronze/industry_sectors      ← crawl_company_info_belong_to_industry_sectors
        bronze/market_type_sectors   ← crawl_company_info_belong_to_market_type
        bronze/industry_info_summary ← crawl_industry_info("summary_info")
        bronze/industry_info_financial ← crawl_industry_info("financial_info")
        bronze/industry_info_fund    ← crawl_industry_info("fund_info")
    """

    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log_lineage(self, source: str, table: str, rows: int) -> None:
        self.context.metadata_repo.log_lineage(
            run_id=self.context.run_id,
            source_layer="external",
            source_table=source,
            target_layer="bronze",
            target_table=table,
            operation="APPEND",
            rows_affected=rows,
        )

    def _warn_dq(self, table: str, count: int) -> None:
        if count > 0:
            self.context.error_log.add(
                ErrorLevel.WARNING,
                f"{count} DQ issues in bronze.{table}",
            )

    # ------------------------------------------------------------------
    # Per-symbol crawlers
    # ------------------------------------------------------------------

    def _ingest_trading_data(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
    ) -> None:
        data = extractor.crawl_trading_data(symbol=symbol, page=1)
        if not data or not data.get("records"):
            self.logger.warning(f"[Bronze] No trading records for {symbol}")
            return
        res = ingester.process(
            raw_records=data["records"],
            batch_id=batch_id,
            run_id=self.context.run_id,
            symbol=symbol,
            cleansing=_build_cleansing_rules(symbol),
        )
        rows = res["stats"]["clean"]
        dq   = res["stats"].get("dq_violations", 0)
        result["rows_ingested"] += rows
        result["dq_issues"] += dq
        self._warn_dq("stock_prices", dq)
        self._log_lineage(f"cophieu68_{symbol}", "bronze_stock_prices", rows)

    def _ingest_company_profile(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
    ) -> None:
        raw = extractor.crawl_company_profile(symbol=symbol)
        if not raw:
            return
        # crawl_company_profile trả về CompanyProfile object hoặc dict
        rec = raw if isinstance(raw, dict) else (raw.__dict__ if hasattr(raw, "__dict__") else None)
        if not rec:
            return
        res = ingester.ingest("company_profile", [rec], batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_company_profile", res["rows"])

    def _ingest_financial_ratios(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
    ) -> None:
        raw = extractor.crawl_financial_ratios(symbol=symbol)
        if not raw:
            return
        rec = raw if isinstance(raw, dict) else (raw.__dict__ if hasattr(raw, "__dict__") else None)
        if not rec:
            return
        res = ingester.ingest("financial_ratios", [rec], batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_financial_ratios", res["rows"])

    def _ingest_financial_report_summary(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
    ) -> None:
        """crawl_financial_report_summary trả về dict{key: StockFinancialReport}
        mỗi StockFinancialReport chứa DataFrame → flatten thành list of dicts."""
        raw = extractor.crawl_financial_report_summary(symbol=symbol)
        if not raw:
            return
        records: List[Dict[str, Any]] = []
        for report_key, report_obj in raw.items():
            df = getattr(report_obj, "data", None)
            if df is None:
                continue
            try:
                sub_records = df.to_dict(orient="records")
                for r in sub_records:
                    r["symbol"] = symbol.upper()
                    r["report_type"] = report_key
                    records.append(r)
            except Exception:
                continue
        if not records:
            return
        res = ingester.ingest("financial_report_summary", records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_financial_report_summary", res["rows"])

    def _ingest_business_plan(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
    ) -> None:
        raw = extractor.crawl_business_plan(symbol=symbol)
        if not raw or not raw.get("data"):
            return
        records = raw["data"]  # list of BusinessPlanRow.__dict__
        res = ingester.ingest("business_plan", records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_business_plan", res["rows"])

    def _ingest_income_statement(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
        report_type: str,
    ) -> None:
        raw = extractor.crawl_details_income_statement(symbol=symbol, report_type=report_type)
        if not raw:
            return
        df = raw.get("data") if isinstance(raw, dict) else getattr(raw, "data", None)
        if df is None:
            return
        try:
            records = df.to_dict(orient="records")
            for r in records:
                r["symbol"] = symbol.upper()
                r["report_type"] = report_type
        except Exception:
            return
        table = f"income_statement_{report_type}"
        res = ingester.ingest(table, records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", f"bronze_{table}", res["rows"])

    def _ingest_balance_sheet(
        self, extractor, ingester: BronzePolarsIngester,
        symbol: str, batch_id: str, result: Dict[str, Any],
        report_type: str,
    ) -> None:
        raw = extractor.crawl_details_balance_sheet(symbol=symbol, report_type=report_type)
        if not raw:
            return
        df = raw.get("data") if isinstance(raw, dict) else getattr(raw, "data", None)
        if df is None:
            return
        try:
            records = df.to_dict(orient="records")
            for r in records:
                r["symbol"] = symbol.upper()
                r["report_type"] = report_type
        except Exception:
            return
        table = f"balance_sheet_{report_type}"
        res = ingester.ingest(table, records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", f"bronze_{table}", res["rows"])

    # ------------------------------------------------------------------
    # Global crawlers (không cần symbol)
    # ------------------------------------------------------------------

    def _ingest_industry_sectors(
        self, extractor, ingester: BronzePolarsIngester,
        batch_id: str, result: Dict[str, Any],
    ) -> None:
        rows = extractor.crawl_company_info_belong_to_industry_sectors()
        if not rows:
            return
        records = [r if isinstance(r, dict) else r.__dict__ for r in rows]
        res = ingester.ingest("industry_sectors", records, batch_id, self.context.run_id)
        result["rows_ingested"] += res["rows"]
        self._log_lineage("cophieu68_global", "bronze_industry_sectors", res["rows"])

    def _ingest_market_type_sectors(
        self, extractor, ingester: BronzePolarsIngester,
        batch_id: str, result: Dict[str, Any],
    ) -> None:
        rows = extractor.crawl_company_info_belong_to_market_type()
        if not rows:
            return
        records = [r if isinstance(r, dict) else r.__dict__ for r in rows]
        res = ingester.ingest("market_type_sectors", records, batch_id, self.context.run_id)
        result["rows_ingested"] += res["rows"]
        self._log_lineage("cophieu68_global", "bronze_market_type_sectors", res["rows"])

    def _ingest_industry_info(
        self, extractor, ingester: BronzePolarsIngester,
        batch_id: str, result: Dict[str, Any],
        type_info: str,
    ) -> None:
        raw = extractor.crawl_industry_info(type_info=type_info)
        if not raw:
            return
        # crawl_industry_info trả về dict{key: row.__dict__}
        records = []
        for key, row in raw.items():
            rec = row if isinstance(row, dict) else row.__dict__
            rec["_industry_key"] = key
            records.append(rec)
        if not records:
            return
        table = f"industry_info_{type_info}"
        res = ingester.ingest(table, records, batch_id, self.context.run_id)
        result["rows_ingested"] += res["rows"]
        self._log_lineage("cophieu68_global", f"bronze_{table}", res["rows"])

    # ------------------------------------------------------------------
    # Main execute
    # ------------------------------------------------------------------

    def execute(self) -> Dict[str, Any]:
        self.logger.info(f"[BronzeExecutor] Starting bronze phase symbols={self.context.symbols}")
        result = {
            "phase": "bronze",
            "symbols_processed": 0,
            "rows_ingested": 0,
            "dq_issues": 0,
            "errors": 0,
        }

        polars_engine = _build_polars_engine(self.config)
        ingester      = BronzePolarsIngester(engine=polars_engine, base_path=LAKEHOUSE_BASE)
        extractor     = _build_extractor(self.config)
        global_batch  = make_batch_id("GLOBAL")

        # ── Per-symbol ────────────────────────────────────────────────
        for symbol in self.context.symbols:
            batch_id = make_batch_id(symbol)
            self.logger.info(f"[BronzeExecutor] symbol={symbol} batch={batch_id}")
            symbol_ok = True
            for _step, _fn in [
                ("trading_data",              lambda: self._ingest_trading_data(extractor, ingester, symbol, batch_id, result)),
                ("company_profile",           lambda: self._ingest_company_profile(extractor, ingester, symbol, batch_id, result)),
                ("financial_ratios",          lambda: self._ingest_financial_ratios(extractor, ingester, symbol, batch_id, result)),
                ("financial_report_summary",  lambda: self._ingest_financial_report_summary(extractor, ingester, symbol, batch_id, result)),
                ("business_plan",             lambda: self._ingest_business_plan(extractor, ingester, symbol, batch_id, result)),
                ("income_statement_quarter",  lambda: self._ingest_income_statement(extractor, ingester, symbol, batch_id, result, "quarter")),
                ("income_statement_year",     lambda: self._ingest_income_statement(extractor, ingester, symbol, batch_id, result, "year")),
                ("balance_sheet_quarter",     lambda: self._ingest_balance_sheet(extractor, ingester, symbol, batch_id, result, "quarter")),
                ("balance_sheet_year",        lambda: self._ingest_balance_sheet(extractor, ingester, symbol, batch_id, result, "year")),
            ]:
                try:
                    _fn()
                except Exception as exc:
                    self.logger.error(f"[BronzeExecutor] {symbol}/{_step} failed: {exc}")
                    self.context.error_log.add(ErrorLevel.WARNING, f"Bronze {symbol}/{_step}: {exc}")
                    symbol_ok = False  # ghi nhận lỗi nhưng tiếp tục các bước còn lại
            result["symbols_processed"] += 1
            if not symbol_ok:
                result["errors"] += 1

        # ── Global (chạy 1 lần) ───────────────────────────────────────
        try:
            self._ingest_industry_sectors(extractor, ingester, global_batch, result)
            self._ingest_market_type_sectors(extractor, ingester, global_batch, result)
            self._ingest_industry_info(extractor, ingester, global_batch, result, "summary_info")
            self._ingest_industry_info(extractor, ingester, global_batch, result, "financial_info")
            self._ingest_industry_info(extractor, ingester, global_batch, result, "fund_info")
        except Exception as exc:
            self.logger.error(f"[BronzeExecutor] Error in global crawlers: {exc}")
            self.context.error_log.add(ErrorLevel.ERROR, f"Bronze global crawl failed: {exc}")
            result["errors"] += 1

        self.logger.info(
            f"[BronzeExecutor] Completed: rows_ingested={result['rows_ingested']} "
            f"symbols={result['symbols_processed']} errors={result['errors']}"
        )
        return result


class SilverExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self) -> Dict[str, Any]:
        self.logger.info(f"[SilverExecutor] Starting silver phase for date={self.context.target_date}")
        result = {
            "phase": "silver",
            "rows_in": 0,
            "rows_out": 0,
            "errors": 0,
        }

        duck_engine = _build_duckdb_engine(self.config)
        polars_engine = _build_polars_engine(self.config)
        processor = SilverProcessor(
            duckdb_engine=duck_engine,
            polars_engine=polars_engine,
            base_path=LAKEHOUSE_BASE,
        )

        # All 10 silver transforms — each runs independently so one failure
        # does not block the rest.
        # bronze_src label, silver_tgt label (matches delta_schema_registry.py), callable
        transforms = [
            ("stock_prices",        "fact_stock_price",        lambda: processor.transform_stock_prices(
                target_date=self.context.target_date,
                run_id=self.context.run_id,
                dedup_strategy=DeduplicationStrategy.KEEP_LAST,
            )),
            ("company_profile",     "dim_company",             lambda: processor.transform_company_profile(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("financial_ratios",    "fact_financial_metrics",  lambda: processor.transform_financial_ratios(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("financial_report",    "fact_financial_report",   lambda: processor.transform_financial_report(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("business_plan",       "fact_business_plan",      lambda: processor.transform_business_plan(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("income_statement",    "fact_income_statement",   lambda: processor.transform_income_statement(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("balance_sheet",       "fact_balance_sheet",      lambda: processor.transform_balance_sheet(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("industry_sectors",    "dim_industry",            lambda: processor.transform_industry_sectors(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("market_type_sectors", "dim_market_type",         lambda: processor.transform_market_type_sectors(
                target_date=self.context.target_date, run_id=self.context.run_id)),
            ("industry_info",       "fact_industry_summary",   lambda: processor.transform_industry_info(
                target_date=self.context.target_date, run_id=self.context.run_id)),
        ]

        try:
            for bronze_src, silver_tgt, fn in transforms:
                try:
                    t_result = fn()
                    rows_out = t_result.get("rows_out", 0)
                    result["rows_in"]  = result.get("rows_in",  0) + t_result.get("rows_in",  0)
                    result["rows_out"] = result.get("rows_out", 0) + rows_out
                    if rows_out > 0:
                        self.context.metadata_repo.log_lineage(
                            run_id=self.context.run_id,
                            source_layer="bronze",
                            source_table=f"bronze_{bronze_src}",
                            target_layer="silver",
                            target_table=f"silver_{silver_tgt}",
                            operation="MERGE",
                            rows_affected=rows_out,
                        )
                    self.logger.info(
                        f"[SilverExecutor] {bronze_src} → {silver_tgt}: "
                        f"rows_in={t_result.get('rows_in',0)} rows_out={rows_out}"
                    )
                except Exception as exc:
                    self.logger.error(f"[SilverExecutor] {bronze_src} failed: {exc}")
                    self.context.error_log.add(
                        ErrorLevel.ERROR,
                        f"Silver {bronze_src} → {silver_tgt} failed: {exc}",
                    )
                    result["errors"] += 1
        finally:
            duck_engine.close()

        self.logger.info(f"[SilverExecutor] Completed silver phase with rows_out={result.get('rows_out', 0)}")
        return result


class GoldExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self) -> Dict[str, Any]:
        self.logger.info(f"[GoldExecutor] Starting gold phase for date={self.context.target_date}")
        result = {
            "phase": "gold",
            "run_status": "UNKNOWN",
            "audits": None,
            "errors": 0,
        }

        sqlmesh_engine = _build_sqlmesh_engine(self.config)
        duck_engine = _build_duckdb_engine(self.config)
        processor = GoldProcessor(sqlmesh_engine=sqlmesh_engine, duck_engine=duck_engine)

        try:
            gold_result = processor.run_gold_models(
                environment=self.context.environment,
                start_date=self.context.target_date,
                end_date=self.context.target_date,
                run_audits=True,
            )
            result.update(gold_result)

            if gold_result.get("run_status") == "SUCCESS":
                self.context.metadata_repo.log_lineage(
                    run_id=self.context.run_id,
                    source_layer="silver",
                    source_table="silver_fact_stock_price",
                    target_layer="gold",
                    target_table="gold_stock_kpis",
                    operation="OVERWRITE",
                    rows_affected=gold_result.get("audits") == "PASSED" and 1 or 0,
                )

        except Exception as exc:
            self.logger.error(f"[GoldExecutor] Error: {exc}")
            self.context.error_log.add(
                ErrorLevel.ERROR,
                f"Gold modeling failed: {exc}",
            )
            result["errors"] += 1

        finally:
            duck_engine.close()

        self.logger.info(f"[GoldExecutor] Completed gold phase with status={result.get('run_status')}")
        return result


class ServingExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self, mart_tables: Optional[List[str]] = None) -> Dict[str, Any]:
        self.logger.info(f"[ServingExecutor] Starting serving phase for date={self.context.target_date}")
        result = {
            "phase": "serving",
            "tables_synced": 0,
            "errors": 0,
            "details": {},
        }

        mart_tables = mart_tables or ["mart_kpi_daily"]
        duck_engine = _build_duckdb_engine(self.config)
        sync_proc = ServingSyncProcessor(duck_engine=duck_engine, base_path=LAKEHOUSE_BASE)

        try:
            for table in mart_tables:
                sync_result = sync_proc.sync_gold_to_postgres(
                    mart_table=table,
                    target_date=self.context.target_date,
                )
                result["details"][table] = sync_result
                if sync_result.get("status") == "SUCCESS":
                    result["tables_synced"] += 1
        except Exception as exc:
            self.logger.error(f"[ServingExecutor] Error: {exc}")
            self.context.error_log.add(
                ErrorLevel.ERROR,
                f"Serving sync failed: {exc}",
            )
            result["errors"] += 1
        finally:
            duck_engine.close()

        sync_proc.optimize_gold_tables()
        self.logger.info(f"[ServingExecutor] Completed serving phase tables_synced={result['tables_synced']}")
        return result


class MasterPipelineOrchestrator:
    def __init__(self, config_path: Optional[str] = None):
        self.logger = logger_manager.get_logger(__name__)
        self.config_manager = ConfigurationManager(config_path, self.logger)
        self.config = self.config_manager.load()
        self.prefect_config = self.config_manager.load_prefect_config()
        logger_manager.configure_from_project_config(str(self.config_manager.config_path))
        self.metadata_repo = MetadataRepository(delta_backend=None, logger=self.logger, in_memory=True)

    def execute(
        self,
        phase: ExecutionPhase,
        symbols: Optional[List[str]] = None,
        backend: str = "polars",
        target_date: Optional[str] = None,
        environment: str = "prod",
        dry_run: bool = False,
        mart_tables: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        symbols = symbols or DEFAULT_SYMBOLS
        backend_enum = ProcessingBackend(backend)
        target_date = target_date or date.today().isoformat()
        run_id = make_run_id()

        context = ExecutionContext(
            run_id=run_id,
            phase=phase,
            symbols=symbols,
            backend=backend_enum,
            target_date=target_date,
            environment=environment,
            dry_run=dry_run,
            metadata_repo=self.metadata_repo,
            error_log=ErrorEventLog(run_id=run_id, job_name=f"etl_{phase.value}"),
            logger=self.logger,
        )

        self.logger.info(
            f"[ORCHESTRATOR] Starting pipeline phase={phase.value} run_id={run_id} target_date={target_date} symbols={symbols} backend={backend}"
        )

        try:
            self.metadata_repo.start_run(
                job_name=f"master_etl_{phase.value}",
                layer=phase.value,
                table_name="all_tables",
                run_id=run_id,
            )

            if dry_run or phase == ExecutionPhase.VALIDATE:
                result = self._validate_pipeline(context)
            else:
                result = self._run_phases(context, mart_tables, environment)

            self.metadata_repo.end_run(run_id, status="SUCCESS", rows_written=None)
            context.status = "SUCCESS"
        except Exception as exc:
            self.logger.error(f"[ORCHESTRATOR] Pipeline failed: {exc}")
            self.metadata_repo.end_run(run_id, status="FAILED", error_message=str(exc))
            context.error_log.add(ErrorLevel.FATAL, f"Pipeline execution failed: {exc}")
            context.status = "FAILED"
            context.error_message = str(exc)
            result = {
                "run_id": run_id,
                "status": "FAILED",
                "error": str(exc),
            }

        context.end_time = datetime.now(timezone.utc)
        self.metadata_repo.log_run_summary(run_id)

        result["duration_seconds"] = context.duration_seconds()
        result["error_events"] = [e.to_dict() for e in context.error_log.events]
        return result

    def _validate_pipeline(self, context: ExecutionContext) -> Dict[str, Any]:
        self.logger.info("[VALIDATION] Running configuration validation")
        validation = self.config_manager.validate()
        if not validation["is_valid"]:
            return {
                "run_id": context.run_id,
                "status": "VALIDATION_FAILED",
                "errors": validation["errors"],
            }
        return {
            "run_id": context.run_id,
            "status": "VALIDATION_PASSED",
            "phase": context.phase.value,
            "symbols": context.symbols,
            "backend": context.backend.value,
            "target_date": context.target_date,
        }

    def _run_phases(
        self,
        context: ExecutionContext,
        mart_tables: Optional[List[str]],
        environment: str,
    ) -> Dict[str, Any]:
        phases: List[ExecutionPhase] = []
        if context.phase == ExecutionPhase.FULL:
            phases = [ExecutionPhase.BRONZE, ExecutionPhase.SILVER, ExecutionPhase.GOLD, ExecutionPhase.SERVING]
        else:
            phases = [context.phase]

        results: Dict[str, Any] = {"run_id": context.run_id, "phases_executed": [], "phase_results": {}}

        for phase in phases:
            phase_result: Dict[str, Any] = {}
            if phase == ExecutionPhase.BRONZE:
                phase_result = BronzeExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.SILVER:
                phase_result = SilverExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.GOLD:
                phase_result = GoldExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.SERVING:
                phase_result = ServingExecutor(context, self.config).execute(mart_tables)

            results["phase_results"][phase.value] = phase_result
            results["phases_executed"].append(phase.value)

        results["status"] = "COMPLETED"
        results["phase_count"] = len(results["phases_executed"])
        results["metrics"] = {
            "total_errors": len(context.error_log.events),
            "error_summary": context.error_log.summary(),
        }
        return results


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified Lakehouse ETL Pipeline Orchestrator")
    parser.add_argument(
        "phase",
        choices=[p.value for p in ExecutionPhase],
        help="Pipeline phase to execute",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help=f"Stock symbols to process (default: {' '.join(DEFAULT_SYMBOLS)})",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Processing date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--backend",
        choices=[b.value for b in ProcessingBackend],
        default="polars",
        help="Processing backend",
    )
    parser.add_argument(
        "--env",
        default="prod",
        choices=["prod", "dev", "staging"],
        help="Environment for SQLMesh models",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to pipeline config YAML",
    )
    parser.add_argument(
        "--marts",
        nargs="*",
        default=None,
        help="Gold mart tables to sync in serving phase",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and skip execution",
    )
    parser.add_argument(
        "--output",
        choices=["json", "text", "summary"],
        default="summary",
        help="Result output format",
    )
    return parser.parse_args()


def format_result(result: Dict[str, Any], format_type: str = "summary") -> str:
    if format_type == "json":
        return json.dumps(result, indent=2, default=str)
    if format_type == "text":
        lines = ["=" * 72, "ETL Pipeline Execution Result", "=" * 72]
        lines.append(f"Run ID: {result.get('run_id', 'N/A')}")
        lines.append(f"Status: {result.get('status', 'UNKNOWN')}")
        lines.append(f"Phase: {result.get('phase', 'N/A')}")
        lines.append(f"Duration: {result.get('duration_seconds', 0):.1f}s")
        if result.get('phases_executed'):
            lines.append(f"Phases executed: {', '.join(result['phases_executed'])}")
        if result.get('error_events'):
            lines.append("Errors:")
            for err in result['error_events'][:5]:
                lines.append(f"  - [{err.get('error_level')}] {err.get('error_message')}")
        if result.get('error'):
            lines.append(f"Error: {result['error']}")
        lines.append("=" * 72)
        return "\n".join(lines)
    return f"{result.get('status', 'UNKNOWN')} | {result.get('phase', 'N/A')} | {result.get('duration_seconds', 0):.1f}s"


def main() -> None:
    args = parse_arguments()
    orchestrator = MasterPipelineOrchestrator(config_path=args.config)
    result = orchestrator.execute(
        phase=ExecutionPhase(args.phase),
        symbols=args.symbols,
        backend=args.backend,
        target_date=args.date,
        environment=args.env,
        dry_run=args.dry_run,
        mart_tables=args.marts,
    )
    print(format_result(result, args.output))
    if result.get("status") not in ("SUCCESS", "COMPLETED", "VALIDATION_PASSED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
