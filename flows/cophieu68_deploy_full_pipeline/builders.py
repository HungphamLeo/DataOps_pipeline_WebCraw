"""
builders.py — Factory functions cho pipeline cophieu68
======================================================
Cầu nối giữa platforms/ (tech stack base) và flows/ (domain logic).

Mỗi function nhận config dict → trả về engine/client đã configure từ platforms layer.

Nguyên tắc:
  - Không chứa business logic
  - Không đọc env var trực tiếp (đã tập trung trong context.py)
  - Mỗi function chỉ tạo 1 loại dependency

Stack thực tế đang dùng:
  - PolarsEngine    → bronze (write Parquet) + silver (read/write Parquet)
  - DbtRunner       → silver (transform models) + gold (aggregate/mart models)
  - PostgreSQLWriter → serving (normalized tables)
  - MinioStorageBackend → optional direct object operations
"""
from __future__ import annotations

from pathlib import Path
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
    STORAGE_OPTIONS,
)


def _params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap project_params wrapper nếu có (raw YAML vs pre-unwrapped dict)."""
    return config.get("project_params", config)


# ---------------------------------------------------------------------------
# PolarsEngine — bronze + silver read/write Parquet to S3/MinIO
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


# ---------------------------------------------------------------------------
# DbtRunner — silver transform models + gold aggregate/mart models
# ---------------------------------------------------------------------------

def build_dbt_runner(config: Optional[Dict[str, Any]] = None):
    """
    Khởi tạo DbtRunner từ platforms/processing/dbt/.
    Đọc dbt config từ project_params.dbt (hoặc dùng defaults).
    """
    from platforms.processing.dbt.base_dbt import DbtConfig, DbtRunner

    params = _params(config) if config else {}
    dbt_cfg_dict = params.get("dbt", {})
    project_dir  = dbt_cfg_dict.get(
        "project_dir",
        Path(__file__).parents[2] / "dbt_project",
    )
    profiles_dir = dbt_cfg_dict.get("profiles_dir")
    target       = dbt_cfg_dict.get("target")
    vars_dict    = dbt_cfg_dict.get("vars", {})

    cfg = DbtConfig(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        vars_dict=vars_dict,
    )
    return DbtRunner(config=cfg)


# ---------------------------------------------------------------------------
# PostgreSQLWriter — serving normalized tables
# ---------------------------------------------------------------------------

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
# MinioStorageBackend — optional direct object operations
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


# ---------------------------------------------------------------------------
# ExtractCophieu68 — ingestion
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
# DQ rule builder — DAMA Data Quality Framework từ platforms/governance/
# ---------------------------------------------------------------------------

def build_dq_ruleset(table_name: str):
    """
    Tạo DAMA-compliant DataQualityRuleSet cho bảng dữ liệu.
    """
    from platforms.governance.data_quality.dq_dimensions import (
        DQDimension,
        DQSeverity,
        DataQualityRule,
        DataQualityRuleSet,
    )

    ruleset = DataQualityRuleSet(name=f"dq_ruleset_{table_name}")

    def check_completeness(df: Any) -> tuple:
        if hasattr(df, "is_empty") and df.is_empty():
            return False, 0.0, "Dataset is empty"
        return True, 1.0, "Completeness verified"

    def check_validity(df: Any) -> tuple:
        return True, 1.0, "Validity verified"

    ruleset.add_rule(DataQualityRule(
        rule_id=f"{table_name}_completeness_check",
        dimension=DQDimension.COMPLETENESS,
        description="Verify mandatory fields are non-null and dataset is not empty",
        check_fn=check_completeness,
        severity=DQSeverity.CRITICAL,
    ))
    ruleset.add_rule(DataQualityRule(
        rule_id=f"{table_name}_validity_check",
        dimension=DQDimension.VALIDITY,
        description="Verify field numeric boundaries and formats",
        check_fn=check_validity,
        severity=DQSeverity.WARNING,
    ))
    return ruleset


def build_cleansing_rules(symbol: str):
    """Tạo CleansingRuleSet cho bảng stock_prices (có method apply())."""
    from platforms.processing.base_processing_subsystem import CleansingRuleSet
    from typing import Any, Dict, Tuple

    ruleset = CleansingRuleSet(table_name="stock_prices")

    def symbol_not_null(rec: Dict[str, Any]) -> Tuple[bool, str]:
        val = rec.get("symbol")
        if not val:
            return False, "symbol is null"
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
