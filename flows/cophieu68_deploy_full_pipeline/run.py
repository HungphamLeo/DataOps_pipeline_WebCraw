"""
flows/cophieu68_deploy_full_pipeline/run.py
============================================
Cophieu68 Pipeline Orchestrator.

Trách nhiệm của file này:
  1. Subclass BasePipelineOrchestrator với các Executors cụ thể của cophieu68.
  2. Expose Cophieu68PipelineOrchestrator cho CLI và Prefect.

Không chứa:
  - Config loading logic (→ Cophieu68PipelineConfig / BasePipelineConfig)
  - run/dispatch loop (→ BasePipelineOrchestrator)
  - Business logic (→ bronze.py / silver.py / gold.py / serving.py)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from flows.common.base_orchestrator import BasePipelineOrchestrator, format_pipeline_result
from flows.common.base_executor import BaseExecutor
from flows.cophieu68_deploy_full_pipeline.pipeline_config import Cophieu68PipelineConfig


class Cophieu68PipelineOrchestrator(BasePipelineOrchestrator):
    """
    Orchestrator cho pipeline cophieu68.

    Kế thừa toàn bộ run/dispatch/validate loop từ BasePipelineOrchestrator.
    Chỉ cần cung cấp:
      - _create_executors() : map phase → executor
      - _default_symbols()  : danh sách mã mặc định
    """

    PIPELINE_NAME = "cophieu68"

    def __init__(self, config_path: Optional[str] = None) -> None:
        config = Cophieu68PipelineConfig(config_path=config_path)
        config.load()
        super().__init__(config=config)

    # ------------------------------------------------------------------
    # BasePipelineOrchestrator hooks
    # ------------------------------------------------------------------

    def _create_executors(
        self,
        context: Any,
        config: Cophieu68PipelineConfig,
    ) -> Dict[str, BaseExecutor]:
        """Khởi tạo tất cả Executors cho pipeline cophieu68."""
        from flows.cophieu68_deploy_full_pipeline.bronze import BronzeExecutor
        from flows.cophieu68_deploy_full_pipeline.silver import SilverExecutor
        from flows.cophieu68_deploy_full_pipeline.gold import GoldExecutor
        from flows.cophieu68_deploy_full_pipeline.serving import ServingExecutor

        return {
            "bronze":  BronzeExecutor(context, config),
            "silver":  SilverExecutor(context, config),
            "gold":    GoldExecutor(context, config),
            "serving": ServingExecutor(context, config),
        }

    def _default_symbols(self) -> List[str]:
        return self.config.DEFAULT_SYMBOLS

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


# ---------------------------------------------------------------------------
# Backward-compat: MasterPipelineOrchestrator alias
# (CLI cũ có thể import tên này)
# ---------------------------------------------------------------------------
MasterPipelineOrchestrator = Cophieu68PipelineOrchestrator

# Re-export format_result cho CLI
format_result = format_pipeline_result
