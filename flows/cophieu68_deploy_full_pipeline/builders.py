"""
builders.py — Factory functions cho pipeline cophieu68
======================================================
Layer này là cầu nối giữa platforms/ (tech stack base) và flows/ (domain logic).

Mỗi function nhận config dict và trả về một engine/client đã được
configure sẵn từ platforms layer. Flow code chỉ gọi builders, không
import trực tiếp từ platforms.

Nguyên tắc:
  - Không chứa business logic
  - Không đọc env var trực tiếp (đã được context.py tập trung)
  - Mỗi function chỉ tạo 1 loại dependency
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from flows.cophieu68_deploy_full_pipeline.context import (
    LAKEHOUSE_BASE,
    PG_DB,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    S3_ENDPOINT,
    S3_KEY,
    S3_SECRET,
    S3_ENDPOINT_URL,
    SQLMESH_GATEWAY,
    SQLMESH_PATH,
    STORAGE_OPTIONS,
)


def _params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap project_params wrapper nếu có (raw YAML vs pre-unwrapped dict)."""
    return config.get("project_params", config)


# ---------------------------------------------------------------------------
# Processing engines — lấy từ platforms/processing/
# ---------------------------------------------------------------------------

def build_polars_engine(config: Dict[str, Any]):
    """Khởi tạo PolarsEngine với S3 storage options từ platforms."""
    from platforms.processing.polars.polars_engine import PolarsConfig, PolarsEngine
    from shared.logger.python_main_logger import logger_manager

    params = _params(config)
    cfg = PolarsConfig(
        thread_pool_size=params.get("polars", {}).get("thread_pool_size"),
        enable_streaming=params.get("polars", {}).get("enable_streaming", True),
        storage_options=STORAGE_OPTIONS,
    )
    return PolarsEngine(
        config=cfg,
        logger=logger_manager.get_logger("polars_engine"),
    )


def build_duckdb_engine(config: Optional[Dict[str, Any]] = None):
    """Khởi tạo DuckDBEngine in-memory với S3 credentials từ platforms."""
    from platforms.processing.duckdb.duckdb_engine import DuckDBConfig, DuckDBEngine
    from shared.logger.python_main_logger import logger_manager

    duck_cfg = DuckDBConfig(
        database_path=":memory:",
        storage_options=STORAGE_OPTIONS,
    )
    return DuckDBEngine(
        config=duck_cfg,
        logger=logger_manager.get_logger("duckdb_engine"),
    )


def build_sqlmesh_engine(config: Optional[Dict[str, Any]] = None):
    """Khởi tạo SqlMeshEngine từ platforms."""
    from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshConfig, SqlMeshEngine
    from shared.logger.python_main_logger import logger_manager

    cfg = SqlMeshConfig(
        project_path=SQLMESH_PATH,
        gateway=SQLMESH_GATEWAY,
    )
    return SqlMeshEngine(
        config=cfg,
        logger=logger_manager.get_logger("sqlmesh_engine"),
    )


# ---------------------------------------------------------------------------
# Storage backends — lấy từ platforms/storage/
# ---------------------------------------------------------------------------

def build_minio_backend():
    """Khởi tạo MinioStorageBackend từ platforms/storage/minio/."""
    from platforms.storage.minio.minio_storage import MinioStorageBackend
    from shared.logger.python_main_logger import logger_manager

    return MinioStorageBackend(
        endpoint=S3_ENDPOINT,
        access_key=S3_KEY,
        secret_key=S3_SECRET,
        secure=False,
        logger=logger_manager.get_logger("minio_storage"),
    )


def build_pg_writer():
    """
    Khởi tạo PostgreSQLWriter từ platforms/storage/postgre/.
    Trả về None nếu PG_USER chưa được set (credentials chưa configure).
    """
    from platforms.storage.postgre.base_postgre import PostgreSQLWriter
    from shared.logger.python_main_logger import logger_manager

    if not PG_USER:
        return None

    return PostgreSQLWriter(
        host=PG_HOST,
        port=int(PG_PORT),
        database=PG_DB,
        username=PG_USER,
        password=PG_PASSWORD,
        pool_min=1,
        pool_max=5,
        logger=logger_manager.get_logger("pg_writer"),
    )


# ---------------------------------------------------------------------------
# Ingestion extractor — lấy từ platforms/ingestion/
# ---------------------------------------------------------------------------

def build_extractor(config: Dict[str, Any]):
    """Khởi tạo ExtractCophieu68 với config từ YAML."""
    from flows.cophieu68_deploy_full_pipeline.ingestion.source.extract_cophieu68 import ExtractCophieu68
    from shared.logger.python_main_logger import logger_manager

    params = _params(config)
    cophieu_cfg = params.get("sources", {}).get("cophieu68", {})
    pipeline_cfg = {
        "sources": {"cophieu68": cophieu_cfg},
        "http": params.get("http", {"delay_seconds": 0.5, "timeout_seconds": 30}),
    }
    return ExtractCophieu68(
        pipeline_config=pipeline_cfg,
        pipeline_logger=logger_manager.get_logger("extractor.cophieu68"),
    )


# ---------------------------------------------------------------------------
# DQ rule builder — domain-specific, thuộc flows không thuộc platforms
# ---------------------------------------------------------------------------

def build_cleansing_rules(symbol: str):
    """Tạo CleansingRuleSet cho bảng stock_prices."""
    from platforms.processing.base_processing_subsystem.subsystem4_data_quality_pre_evaluate import (
        CleansingRuleSet,
    )
    from typing import Any, Dict, Tuple

    ruleset = CleansingRuleSet(table_name="stock_prices")

    def symbol_not_null(rec: Dict[str, Any]) -> Tuple[bool, str]:
        val = rec.get("symbol")
        if not val:
            return False, f"symbol is null — record: {rec}"
        return True, ""

    def positive_close(rec: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            close = float(rec.get("close_price") or rec.get("close") or 0)
            if close <= 0:
                return False, f"close_price <= 0: {close}"
        except (TypeError, ValueError):
            return False, f"close_price không parse được: {rec.get('close_price')}"
        return True, ""

    def non_negative_volume(rec: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            vol = float(rec.get("volume") or 0)
            if vol < 0:
                return False, f"volume < 0: {vol}"
        except (TypeError, ValueError):
            pass
        return True, ""

    ruleset.add_rule(symbol_not_null)
    ruleset.add_rule(positive_close)
    ruleset.add_rule(non_negative_volume)
    return ruleset
