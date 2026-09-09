"""
gold.py — Gold Phase Module
============================
Chạy SQLMesh models: Silver Parquet (MinIO) → Gold Parquet (MinIO).

Dùng:
  - platforms/processing/sqlmesh/  → SqlMeshEngine
  - platforms/processing/duckdb/   → DuckDBEngine (audit queries)

Không chứa:
  - Crawl web, transform raw data (bronze/silver)
  - Sync vào PostgreSQL (thuộc serving.py)
  - CLI / orchestration (thuộc run.py)
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from flows.cophieu68_deploy_full_pipeline.context import ExecutionContext
from flows.cophieu68_deploy_full_pipeline.builders import (
    build_sqlmesh_engine,
    build_duckdb_engine,
)


class GoldProcessor:
    """
    Chạy SQLMesh plan + run + audit để materialise Gold models.
    Silver Parquet → Gold Parquet trên MinIO.
    """

    def __init__(self, sqlmesh_engine, duck_engine) -> None:
        self.sqlmesh = sqlmesh_engine
        self.duck    = duck_engine
        self.logger  = sqlmesh_engine.logger

    def run_gold_models(
        self,
        environment: str = "prod",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        run_audits: bool = True,
    ) -> Dict[str, Any]:
        self.logger.info(
            "[Gold] Running SQLMesh models env=%s start=%s end=%s",
            environment, start_date, end_date,
        )
        result: Dict[str, Any] = {"environment": environment}

        # Step 1: plan + backfill
        try:
            self.sqlmesh.plan(environment=environment)
            result["plan"] = "applied"
            self.logger.info("[Gold] SQLMesh plan+backfill complete")
        except Exception as exc:
            msg = str(exc)
            # S3/MinIO errors khi silver chưa có data — treat as SKIPPED
            if any(kw in msg for kw in ("Connection error", "IO Error", "Plan application")):
                self.logger.warning(
                    "[Gold] Skipped — silver not ready or MinIO offline: %s", msg[:120]
                )
                result["plan"] = "skipped_no_silver_data"
                result["run_status"] = "SKIPPED"
                return result
            raise

        # Step 2: incremental run
        try:
            self.sqlmesh.run(environment=environment, start=start_date, end=end_date)
            result["run_status"] = "SUCCESS"
            self.logger.info("[Gold] SQLMesh run complete")
        except Exception as exc:
            self.logger.warning("[Gold] SQLMesh run warning: %s", exc)
            result["run_status"] = f"WARN: {exc}"

        # Step 3: audit
        if run_audits:
            try:
                self.sqlmesh.audit()
                result["audits"] = "PASSED"
                self.logger.info("[Gold] SQLMesh audits complete")
            except Exception as exc:
                self.logger.warning("[Gold] Audit warning: %s", exc)
                result["audits"] = f"WARN: {exc}"

        return result


class GoldExecutor:
    """Orchestrate Gold phase cho pipeline cophieu68."""

    def __init__(self, context: ExecutionContext, config: Dict[str, Any]) -> None:
        self.context = context
        self.config  = config
        self.logger  = context.logger

    def execute(self) -> Dict[str, Any]:
        from platforms.processing.base_processing_subsystem import ErrorLevel

        self.logger.info("[GoldExecutor] Starting gold phase date=%s", self.context.target_date)
        result = {"phase": "gold", "run_status": "UNKNOWN", "audits": None, "errors": 0}

        sqlmesh = build_sqlmesh_engine(self.config)
        duck    = build_duckdb_engine(self.config)
        proc    = GoldProcessor(sqlmesh_engine=sqlmesh, duck_engine=duck)

        try:
            gold_result = proc.run_gold_models(
                environment=self.context.environment,
                start_date=self.context.target_date,
                end_date=self.context.target_date,
                run_audits=True,
            )
            result.update(gold_result)

            if gold_result.get("run_status") == "SUCCESS":
                self.context.metadata_repo.log_lineage(
                    run_id=self.context.run_id,
                    source_layer="silver", source_table="silver_fact_stock_price",
                    target_layer="gold",   target_table="gold_stock_kpis",
                    operation="OVERWRITE",
                    rows_affected=1 if gold_result.get("audits") == "PASSED" else 0,
                )
        except Exception as exc:
            self.logger.error("[GoldExecutor] Error: %s", exc)
            self.context.error_log.add(ErrorLevel.ERROR, f"Gold modeling failed: {exc}")
            result["errors"] += 1
        finally:
            duck.close()

        self.logger.info("[GoldExecutor] Done status=%s", result.get("run_status"))
        return result
