"""
run.py — Master Pipeline Orchestrator
======================================
Orchestrator duy nhất cho pipeline cophieu68.
Gọi Bronze → Silver → Gold → Serving theo thứ tự.

Trách nhiệm:
  - Load config từ YAML
  - Khởi tạo ExecutionContext
  - Dispatch sang từng Executor theo phase
  - Ghi metadata / lineage

Không chứa business logic — chỉ wire và điều phối.
CLI entry point nằm trong cli/main.py.
"""
from __future__ import annotations

import json
import logging
import yaml
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from shared.logger.python_main_logger import logger_manager

from flows.cophieu68_deploy_full_pipeline.context import (
    ExecutionContext,
    ExecutionPhase,
    ProcessingBackend,
    DEFAULT_SYMBOLS,
    DEFAULT_CONFIG_PATH,
    make_run_id,
)
from flows.cophieu68_deploy_full_pipeline.bronze import BronzeExecutor
from flows.cophieu68_deploy_full_pipeline.silver import SilverExecutor
from flows.cophieu68_deploy_full_pipeline.gold import GoldExecutor
from flows.cophieu68_deploy_full_pipeline.serving import ServingExecutor


class ConfigurationManager:
    """
    Load và validate YAML config.
    SRP: chỉ quan tâm đến cấu hình, không chứa pipeline logic.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.logger      = logger or logger_manager.get_logger(__name__)
        self.config_path = Path(config_path or DEFAULT_CONFIG_PATH)
        self.config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f) or {}
            self.logger.info("[Config] Loaded from %s", self.config_path)
            return self.config
        except FileNotFoundError:
            self.logger.error("[Config] File not found: %s", self.config_path)
            return {}
        except Exception as exc:
            self.logger.error("[Config] Load error: %s", exc)
            return {}

    def validate(self) -> Dict[str, Any]:
        errors = []
        params = self.config.get("project_params", self.config)
        for section in ("sources", "http"):
            if section not in params:
                errors.append(f"Missing section: project_params.{section}")
        if not params.get("sources", {}).get("cophieu68", {}).get("base_url"):
            errors.append("Missing project_params.sources.cophieu68.base_url")
        return {"is_valid": not errors, "errors": errors}


class MasterPipelineOrchestrator:
    """
    Orchestrate toàn bộ pipeline cophieu68.
    CLI gọi orchestrator.run(...), không gọi Executor trực tiếp.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.logger        = logger_manager.get_logger(__name__)
        self.config_mgr    = ConfigurationManager(config_path, self.logger)
        self.config        = self.config_mgr.load()

        logger_manager.configure_from_project_config(str(self.config_mgr.config_path))

        # Lazy import để tránh circular import
        from platforms.processing.base_processing_subsystem import MetadataRepository
        self.metadata_repo = MetadataRepository(
            delta_backend=None, logger=self.logger, in_memory=True
        )

    def run(
        self,
        phase: "str | ExecutionPhase",  # accepts raw string from CLI or ExecutionPhase enum
        symbols: Optional[List[str]] = None,
        backend: str = "polars",
        target_date: Optional[str] = None,
        environment: str = "prod",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        from platforms.processing.base_processing_subsystem import ErrorEventLog, ErrorLevel

        # Resolve raw string → ExecutionPhase (CLI passes string, internal callers may pass enum)
        phase = ExecutionPhase(phase) if not isinstance(phase, ExecutionPhase) else phase
        symbols     = symbols or DEFAULT_SYMBOLS
        target_date = target_date or date.today().isoformat()
        run_id      = make_run_id()

        context = ExecutionContext(
            run_id=run_id,
            phase=phase,
            symbols=symbols,
            backend=ProcessingBackend(backend),
            target_date=target_date,
            environment=environment,
            dry_run=dry_run,
            metadata_repo=self.metadata_repo,
            error_log=ErrorEventLog(run_id=run_id, job_name=f"etl_{phase.value}"),
            logger=self.logger,
        )

        self.logger.info(
            "[Orchestrator] run_id=%s phase=%s date=%s symbols=%s",
            run_id, phase.value, target_date, symbols,
        )

        try:
            self.metadata_repo.start_run(
                job_name=f"master_etl_{phase.value}",
                layer=phase.value,
                table_name="all_tables",
                run_id=run_id,
            )

            if dry_run or phase == ExecutionPhase.VALIDATE:
                result = self._validate(context)
            else:
                result = self._dispatch(context)

            self.metadata_repo.end_run(run_id, status="SUCCESS", rows_written=None)
            context.status = "SUCCESS"

        except Exception as exc:
            self.logger.error("[Orchestrator] Pipeline failed: %s", exc)
            self.metadata_repo.end_run(run_id, status="FAILED", error_message=str(exc))
            context.error_log.add(ErrorLevel.FATAL, f"Pipeline failed: {exc}")
            context.status = "FAILED"
            result = {"run_id": run_id, "status": "FAILED", "error": str(exc)}

        from datetime import datetime, timezone
        context.end_time = datetime.now(timezone.utc)
        self.metadata_repo.log_run_summary(run_id)

        result["run_id"]           = run_id
        result["duration_seconds"] = context.duration_seconds()
        result["error_events"]     = [e.to_dict() for e in context.error_log.events]
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _validate(self, context: ExecutionContext) -> Dict[str, Any]:
        v = self.config_mgr.validate()
        if not v["is_valid"]:
            return {"status": "VALIDATION_FAILED", "errors": v["errors"]}
        return {
            "status":      "VALIDATION_PASSED",
            "phase":       context.phase.value,
            "symbols":     context.symbols,
            "target_date": context.target_date,
        }

    def _dispatch(self, context: ExecutionContext) -> Dict[str, Any]:
        phases: List[ExecutionPhase] = (
            [ExecutionPhase.BRONZE, ExecutionPhase.SILVER,
             ExecutionPhase.GOLD,   ExecutionPhase.SERVING]
            if context.phase == ExecutionPhase.FULL
            else [context.phase]
        )

        results: Dict[str, Any] = {
            "status": "COMPLETED",
            "phases_executed": [],
            "phase_results": {},
        }

        for phase in phases:
            if phase == ExecutionPhase.BRONZE:
                res = BronzeExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.SILVER:
                res = SilverExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.GOLD:
                res = GoldExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.SERVING:
                res = ServingExecutor(context, self.config).execute()
            else:
                continue

            results["phase_results"][phase.value] = res
            results["phases_executed"].append(phase.value)

        results["metrics"] = {
            "total_errors": len(context.error_log.events),
            "error_summary": context.error_log.summary(),
        }
        return results


# ---------------------------------------------------------------------------
# Formatting helper (dùng bởi cli/main.py)
# ---------------------------------------------------------------------------

def format_result(result: Dict[str, Any], fmt: str = "summary") -> str:
    if fmt == "json":
        return json.dumps(result, indent=2, default=str)
    if fmt == "text":
        lines = [
            "=" * 70,
            "Cophieu68 ETL Pipeline",
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
