"""
flows/cophieu68_deploy_full_pipeline/run.py
============================================
Entry point duy nhất của pipeline cophieu68.

Trách nhiệm:
  - Định nghĩa Cophieu68PipelineConfig (subclass BasePipelineConfig).
    Đây là nơi DUY NHẤT load config từ YAML — không file nào khác load config.
  - Định nghĩa Cophieu68PipelineOrchestrator (subclass BasePipelineOrchestrator).
    Inject config đã load xuống tất cả Executors.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from flows.common.base_config import (
    BasePipelineConfig,
    BasePipelineOrchestrator,
    BaseExecutor,
    format_pipeline_result,
)


# ---------------------------------------------------------------------------
# Cophieu68PipelineConfig — nạp từ cophieu68_config.yaml
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parent
    / "ingestion" / "config" / "cophieu68_config.yaml"
)


class Cophieu68PipelineConfig(BasePipelineConfig):
    """
    Config cho pipeline cophieu68.

    Load một lần duy nhất tại Cophieu68PipelineOrchestrator.__init__().
    Tất cả executors nhận config này qua constructor — không tự load lại.
    """

    PIPELINE_NAME        = "cophieu68"
    DEFAULT_CONFIG_PATH  = _DEFAULT_CONFIG_PATH
    REQUIRED_YAML_SECTIONS = ["sources"]

    # Mã cổ phiếu mặc định khi CLI không truyền --symbols
    DEFAULT_SYMBOLS: List[str] = ["HPG", "FPT", "VNM"]


# ---------------------------------------------------------------------------
# Cophieu68PipelineOrchestrator
# ---------------------------------------------------------------------------

class Cophieu68PipelineOrchestrator(BasePipelineOrchestrator):
    """Orchestrator cho pipeline cophieu68."""

    PIPELINE_NAME = "cophieu68"

    def __init__(self, config_path: Optional[str] = None) -> None:
        config = Cophieu68PipelineConfig(config_path=config_path)
        config.load()
        super().__init__(config=config)

    def _create_executors(
        self,
        context: Any,
        config: BasePipelineConfig,
    ) -> Dict[str, BaseExecutor]:
        from flows.cophieu68_deploy_full_pipeline.bronze import BronzeExecutor
        from flows.cophieu68_deploy_full_pipeline.silver import SilverExecutor
        from flows.cophieu68_deploy_full_pipeline.serving import ServingExecutor

        return {
            "bronze":  BronzeExecutor(context, config),
            "silver":  SilverExecutor(context, config),
            "serving": ServingExecutor(context, config),
        }

    def _default_symbols(self) -> List[str]:
        return self.config.DEFAULT_SYMBOLS

    def _phase_order(self) -> List[str]:
        return ["bronze", "silver", "serving"]

    def _validate_config(self, context: Any) -> Dict[str, Any]:
        v = self.config.validate()
        if not v["is_valid"]:
            return {"status": "VALIDATION_FAILED", "errors": v["errors"]}
        return {
            "status":      "VALIDATION_PASSED",
            "pipeline":    self.PIPELINE_NAME,
            "phase":       context.phase.value,
            "symbols":     context.symbols,
            "target_date": context.target_date,
        }


# Backward-compat alias
MasterPipelineOrchestrator = Cophieu68PipelineOrchestrator
format_result = format_pipeline_result
