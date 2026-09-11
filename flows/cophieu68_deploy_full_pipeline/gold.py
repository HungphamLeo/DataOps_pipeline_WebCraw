"""
gold.py — Gold Phase Module
============================
Silver Parquet (MinIO) → dbt gold models → Normalized PostgreSQL.

Luồng:
  - DbtRunner.run(select="gold")   chạy các dbt aggregate/mart models
  - DbtRunner.test(select="gold")  chạy dbt data tests (tùy chọn)

Dùng:
  - platforms/processing/dbt/ → DbtRunner (thay SQLMesh)

Không chứa:
  - SQLMesh (đã loại bỏ — không dùng trong stack này)
  - DuckDB engine
  - Crawl web / transform raw (bronze/silver)
  - CLI / orchestration (run.py)
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from platforms.factory.client_factory import build_dbt_runner

from flows.common.context import ExecutionContext
from flows.cophieu68_deploy_full_pipeline.pipeline_config import Cophieu68PipelineConfig


class GoldProcessor:
    """
    Chạy dbt gold models để materialise Gold layer.
    Silver Parquet / Silver PG → Gold normalized tables.
    """

    def __init__(self, dbt_runner) -> None:
        self.dbt    = dbt_runner
        self.logger = dbt_runner.logger

    def run_gold_models(
        self,
        environment: str = "prod",
        run_tests: bool = True,
        select: str = "gold",
    ) -> Dict[str, Any]:
        """
        Chạy dbt gold models + optional tests.
        select: 'gold' = tất cả models trong models/gold/
        """
        self.logger.info("[Gold] Running dbt models select=%s env=%s", select, environment)
        result: Dict[str, Any] = {"environment": environment, "select": select}

        # Step 1: dbt run
        run_res = self.dbt.run(select=select)
        result["run_status"]     = "SUCCESS" if run_res.success else "FAILED"
        result["run_elapsed"]    = run_res.elapsed_seconds
        result["run_returncode"] = run_res.returncode

        if not run_res.success:
            self.logger.error(
                "[Gold] dbt run FAILED (rc=%d): %s",
                run_res.returncode, run_res.stderr[:500],
            )
            result["run_status"] = f"FAILED rc={run_res.returncode}"
            return result

        self.logger.info("[Gold] dbt run completed in %.1fs", run_res.elapsed_seconds)

        # Step 2: dbt test (optional)
        if run_tests:
            test_res = self.dbt.test(select=select)
            result["test_status"]     = "PASSED" if test_res.success else f"FAILED rc={test_res.returncode}"
            result["test_elapsed"]    = test_res.elapsed_seconds
            if not test_res.success:
                self.logger.warning("[Gold] dbt test failures: %s", test_res.stderr[:300])
            else:
                self.logger.info("[Gold] dbt tests passed in %.1fs", test_res.elapsed_seconds)

        return result


class GoldExecutor:
    """Orchestrate Gold phase cho pipeline cophieu68."""

    def __init__(
        self,
        context: ExecutionContext,
        config: "Cophieu68PipelineConfig",
    ) -> None:
        self.context = context
        self.config  = config
        self.logger  = context.logger

    def execute(self) -> Dict[str, Any]:
        from platforms.processing.base_processing_subsystem import ErrorLevel

        self.logger.info("[GoldExecutor] Starting gold phase date=%s", self.context.target_date)
        result = {"phase": "gold", "run_status": "UNKNOWN", "errors": 0}

        dbt  = build_dbt_runner(**self.config.dbt_build_params)
        proc = GoldProcessor(dbt_runner=dbt)

        try:
            gold_result = proc.run_gold_models(
                environment=self.context.environment,
                run_tests=True,
                select="gold",
            )
            result.update(gold_result)

            if gold_result.get("run_status") == "SUCCESS":
                self.context.metadata_repo.log_lineage(
                    run_id=self.context.run_id,
                    source_layer="silver", source_table="silver_*",
                    target_layer="gold",   target_table="gold_*",
                    operation="OVERWRITE",
                    rows_affected=0,   # dbt không trả về row count
                )
        except Exception as exc:
            self.logger.error("[GoldExecutor] Error: %s", exc)
            self.context.error_log.add(ErrorLevel.ERROR, f"Gold modeling failed: {exc}")
            result["errors"] += 1

        self.logger.info("[GoldExecutor] Done status=%s", result.get("run_status"))
        return result
