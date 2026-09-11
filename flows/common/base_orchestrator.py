"""
flows/shared/base_orchestrator.py
===================================
BasePipelineOrchestrator — vòng lặp run/dispatch/validate chung cho mọi pipeline.

Tại sao tách ra shared/:
  - MasterPipelineOrchestrator trong run.py cũ chứa cả: load config, init context,
    dispatch phase, ghi metadata → vi phạm SRP.
  - BasePipelineOrchestrator tách phần "vòng lặp điều phối" ra riêng (SRP).
  - Pipeline cụ thể (Cophieu68) chỉ cần subclass, cung cấp _get_executor() và
    _build_executors() — không viết lại toàn bộ run/dispatch logic.

Nguyên tắc:
  - Template Method Pattern: run() là template cố định, _dispatch() là hook.
  - DIP: nhận BasePipelineConfig qua constructor, không biết Cophieu68Config.
  - OCP: thêm pipeline mới = tạo subclass mới, không sửa base.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Type

from flows.common.base_config import BasePipelineConfig
from flows.common.base_executor import BaseExecutor


class BasePipelineOrchestrator:
    """
    Template orchestrator cho tất cả pipelines.

    Subclass bắt buộc override:
      - _create_executors(context, config) → Dict[str, BaseExecutor]
            Trả về dict phase → executor instance.
            Ví dụ: {"bronze": BronzeExecutor(ctx, cfg), "silver": SilverExecutor(ctx, cfg)}

    Subclass có thể override:
      - _phase_order() → List[str]
            Thứ tự các phase khi chạy FULL. Mặc định: bronze, silver, gold, serving.
      - _validate_config(config) → Dict[str, Any]
            Logic validate config của từng pipeline.
    """

    PIPELINE_NAME: str = "base"

    def __init__(
        self,
        config: BasePipelineConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        from shared.logger.python_main_logger import logger_manager

        self.config = config
        self.logger = logger or logger_manager.get_logger(
            f"orchestrator.{self.PIPELINE_NAME}"
        )
        self._metadata_repo = self._init_metadata_repo()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        phase: str,
        symbols: Optional[List[str]] = None,
        backend: str = "polars",
        target_date: Optional[str] = None,
        environment: str = "prod",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Entry point duy nhất. Gọi từ CLI hoặc Prefect flow.

        Parameters
        ----------
        phase       : "bronze" | "silver" | "gold" | "serving" | "full" | "validate"
        symbols     : List mã cổ phiếu. None = dùng default của pipeline.
        backend     : "polars" | "dbt"
        target_date : ISO date string "YYYY-MM-DD". None = hôm nay.
        environment : "dev" | "prod"
        dry_run     : Nếu True, chỉ validate config, không chạy thật.
        """
        from flows.common.context import (
            ExecutionContext, ExecutionPhase, ProcessingBackend, make_run_id
        )
        from platforms.processing.base_processing_subsystem import ErrorEventLog, ErrorLevel

        phase_enum  = ExecutionPhase(phase) if isinstance(phase, str) else phase
        symbols     = symbols or self._default_symbols()
        target_date = target_date or date.today().isoformat()
        run_id      = make_run_id()

        context = ExecutionContext(
            run_id=run_id,
            phase=phase_enum,
            symbols=symbols,
            backend=ProcessingBackend(backend),
            target_date=target_date,
            environment=environment,
            dry_run=dry_run,
            metadata_repo=self._metadata_repo,
            error_log=ErrorEventLog(
                run_id=run_id,
                job_name=f"etl_{self.PIPELINE_NAME}_{phase_enum.value}",
            ),
            logger=self.logger,
        )

        self.logger.info(
            "[%s] run_id=%s phase=%s date=%s symbols=%s",
            self.PIPELINE_NAME, run_id, phase_enum.value, target_date, symbols,
        )

        try:
            self._metadata_repo.start_run(
                job_name=f"{self.PIPELINE_NAME}_{phase_enum.value}",
                layer=phase_enum.value,
                table_name="all_tables",
                run_id=run_id,
            )

            if dry_run or phase_enum.value == "validate":
                result = self._run_validate(context)
            else:
                result = self._dispatch(context)

            self._metadata_repo.end_run(run_id, status="SUCCESS", rows_written=None)
            context.status = "SUCCESS"

        except Exception as exc:
            self.logger.error("[%s] Pipeline failed: %s", self.PIPELINE_NAME, exc)
            self._metadata_repo.end_run(run_id, status="FAILED", error_message=str(exc))
            context.error_log.add(ErrorLevel.FATAL, f"Pipeline failed: {exc}")
            context.status  = "FAILED"
            result = {"run_id": run_id, "status": "FAILED", "error": str(exc)}

        context.end_time = datetime.now(timezone.utc)
        self._metadata_repo.log_run_summary(run_id)

        result["run_id"]           = run_id
        result["duration_seconds"] = context.duration_seconds()
        result["error_events"]     = [e.to_dict() for e in context.error_log.events]
        return result

    # ------------------------------------------------------------------
    # Hooks — subclass override
    # ------------------------------------------------------------------

    def _create_executors(
        self,
        context: Any,
        config: BasePipelineConfig,
    ) -> Dict[str, BaseExecutor]:
        """
        Trả về dict {phase_name: executor_instance}.
        Subclass PHẢI override method này.

        Ví dụ:
            return {
                "bronze":  BronzeExecutor(context, config),
                "silver":  SilverExecutor(context, config),
                "gold":    GoldExecutor(context, config),
                "serving": ServingExecutor(context, config),
            }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} phải implement _create_executors()"
        )

    def _phase_order(self) -> List[str]:
        """Thứ tự chạy các phase khi phase == FULL."""
        return ["bronze", "silver", "gold", "serving"]

    def _default_symbols(self) -> List[str]:
        """Default symbols khi caller không truyền vào. Subclass override."""
        return []

    def _validate_config(self, context: Any) -> Dict[str, Any]:
        """Validate config. Subclass override để thêm domain-specific checks."""
        v = self.config.validate()
        if not v["is_valid"]:
            return {"status": "VALIDATION_FAILED", "errors": v["errors"]}
        return {
            "status":      "VALIDATION_PASSED",
            "phase":       context.phase.value,
            "symbols":     context.symbols,
            "target_date": context.target_date,
        }

    # ------------------------------------------------------------------
    # Internal — không override
    # ------------------------------------------------------------------

    def _run_validate(self, context: Any) -> Dict[str, Any]:
        return self._validate_config(context)

    def _dispatch(self, context: Any) -> Dict[str, Any]:
        from flows.common.context import ExecutionPhase

        phases = (
            self._phase_order()
            if context.phase == ExecutionPhase.FULL
            else [context.phase.value]
        )

        executors = self._create_executors(context, self.config)

        results: Dict[str, Any] = {
            "status":          "COMPLETED",
            "phases_executed": [],
            "phase_results":   {},
        }

        for phase_name in phases:
            executor = executors.get(phase_name)
            if executor is None:
                self.logger.warning(
                    "[%s] No executor for phase=%s — skipping",
                    self.PIPELINE_NAME, phase_name,
                )
                continue

            executor.pre_execute()
            res = executor.execute()
            executor.post_execute(res)

            results["phase_results"][phase_name] = res
            results["phases_executed"].append(phase_name)

        results["metrics"] = {
            "total_errors": len(context.error_log.events),
            "error_summary": context.error_log.summary(),
        }
        return results

    def _init_metadata_repo(self):
        from platforms.processing.base_processing_subsystem import MetadataRepository
        return MetadataRepository(
            delta_backend=None,
            logger=self.logger,
            in_memory=True,
        )


# ---------------------------------------------------------------------------
# Formatting helper — dùng bởi CLI (pipeline-agnostic)
# ---------------------------------------------------------------------------

def format_pipeline_result(result: Dict[str, Any], fmt: str = "summary") -> str:
    """Format kết quả pipeline thành string. Dùng được cho tất cả pipelines."""
    if fmt == "json":
        return json.dumps(result, indent=2, default=str)

    if fmt == "text":
        pipeline = result.get("pipeline", "Pipeline")
        lines = [
            "=" * 70,
            pipeline,
            "=" * 70,
            f"Run ID  : {result.get('run_id', 'N/A')}",
            f"Status  : {result.get('status', 'UNKNOWN')}",
            f"Duration: {result.get('duration_seconds', 0):.1f}s",
        ]
        if result.get("phases_executed"):
            lines.append(f"Phases  : {', '.join(result['phases_executed'])}")
        for ev in result.get("error_events", [])[:5]:
            lines.append(f"  [{ev.get('error_level')}] {ev.get('error_message')}")
        lines.append("=" * 70)
        return "\n".join(lines)

    status = result.get("status", "UNKNOWN")
    dur    = result.get("duration_seconds", 0)
    return f"{status} | {dur:.1f}s"
