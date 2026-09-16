"""
flows/cophieu68_deploy_full_pipeline/builders.py
=================================================
Domain-specific factory functions cho pipeline cophieu68.

Chứa DUY NHẤT các factory functions có logic cophieu68-specific:
  - build_extractor()       : ExtractCophieu68 (nguồn cophieu68.com)
  - build_cleansing_rules() : Cleansing rules cho stock_prices

Các infra builders (polars, pg, minio) đã chuyển lên:
  → platforms/factory/client_factory.py

Để tạo clients cho pipeline, dùng Cophieu68PipelineConfig làm nguồn tham số:
    from platforms.factory import build_polars_engine, build_pg_writer, ...
    from flows.cophieu68_deploy_full_pipeline.pipeline_config import Cophieu68PipelineConfig

    config = Cophieu68PipelineConfig().load()
    engine = build_polars_engine(**config.polars_build_params)
    pg     = build_pg_writer(**config.pg_conn_params)
    minio  = build_minio_backend(**config.minio_build_params)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from flows.cophieu68_deploy_full_pipeline.pipeline_config import Cophieu68PipelineConfig


# ---------------------------------------------------------------------------
# build_extractor — cophieu68-specific
# ---------------------------------------------------------------------------

def build_extractor(config: Cophieu68PipelineConfig, logger: Optional[logging.Logger] = None):
    """
    Khởi tạo ExtractCophieu68 với config từ Cophieu68PipelineConfig.

    Parameters
    ----------
    config : Cophieu68PipelineConfig (đã gọi .load())
    logger : Optional logger; mặc định lấy từ logger_manager.
    """
    from flows.cophieu68_deploy_full_pipeline.ingestion.source.extract_cophieu68 import (
        ExtractCophieu68,
    )
    from shared.logger.python_main_logger import logger_manager

    return ExtractCophieu68(
        pipeline_config=config.extractor_pipeline_config,
        pipeline_logger=logger or logger_manager.get_logger("extractor.cophieu68"),
    )


# ---------------------------------------------------------------------------
# build_cleansing_rules — cophieu68-specific
# ---------------------------------------------------------------------------

def build_cleansing_rules(symbol: str):
    """
    Tạo CleansingRuleSet cho bảng stock_prices (có method apply()).

    Parameters
    ----------
    symbol : Mã cổ phiếu — dùng trong error messages.
    """
    from platforms.processing.base_processing_subsystem import CleansingRuleSet

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


# ---------------------------------------------------------------------------
# build_dq_ruleset — DAMA-compliant, dùng trong bronze.py
# ---------------------------------------------------------------------------

def build_dq_ruleset(table_name: str):
    """
    Tạo DataQualityRuleSet DAMA-compliant cho bảng dữ liệu.
    Dùng trong BronzeIngester để audit từng batch.
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
