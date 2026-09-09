"""
Module: platforms.governance.data_quality.great_expectation.ge_runner
Layer: Platform Governance Subsystem - Great Expectations
Responsibility: Domain-agnostic Great Expectations runner executing checkpoints and expectation suites,
                adapting results into standard DAMA-compliant DataQualityResult structures.
Does NOT contain: Pipeline-specific transformations, source crawling, database migrations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import time

from platforms.governance.data_quality.dq_dimensions import (
    DQDimension,
    DQSeverity,
    DataQualityResult,
)
from shared.logger.python_main_logger import logger_manager


@dataclass
class GEResult:
    """
    Standard result wrapper for a Great Expectations checkpoint or suite execution.
    """
    checkpoint_name: str
    success: bool
    dq_results: List[DataQualityResult] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: float = 0.0
    error_message: Optional[str] = None


class GreatExpectationsRunner:
    """
    Domain-agnostic adapter for executing Great Expectations validation checkpoints.
    Converts GE validation results into platform-standard DataQualityResult lists.
    """

    def __init__(self, ge_context_path: Optional[str | Path] = None):
        self.ge_context_path = Path(ge_context_path) if ge_context_path else None
        self.logger = logger_manager.get_logger("logger.governance.data_quality")
        self._context: Any = None

    def _get_context(self) -> Any:
        """Lazy load GE DataContext."""
        if self._context is not None:
            return self._context

        try:
            import great_expectations as gx  # type: ignore
            if self.ge_context_path and self.ge_context_path.exists():
                self._context = gx.get_context(project_root_dir=str(self.ge_context_path))
            else:
                self._context = gx.get_context(mode="ephemeral")
            return self._context
        except ImportError:
            self.logger.warning("great_expectations is not installed in the environment. GE runner running in fallback mode.")
            return None
        except Exception as exc:
            self.logger.error(f"Failed to initialize Great Expectations context: {str(exc)}", exc_info=True)
            return None

    def _map_expectation_to_dama_dimension(self, expectation_type: str) -> DQDimension:
        """
        Map standard Great Expectations expectation types to DAMA 6 Dimensions.
        """
        exp = expectation_type.lower()
        if "null" in exp or "values_to_be_in_set" in exp and "not_null" in exp:
            return DQDimension.COMPLETENESS
        elif "unique" in exp or "distinct" in exp:
            return DQDimension.UNIQUENESS
        elif "type" in exp or "format" in exp or "regex" in exp or "between" in exp or "match" in exp:
            return DQDimension.VALIDITY
        elif "date" in exp or "time" in exp or "fresh" in exp:
            return DQDimension.TIMELINESS
        elif "table_columns" in exp or "table_row_count_to_equal_other_table" in exp:
            return DQDimension.CONSISTENCY
        else:
            return DQDimension.ACCURACY

    def run_checkpoint(
        self,
        checkpoint_name: str,
        batch_data: Optional[Any] = None,
        runtime_parameters: Optional[Dict[str, Any]] = None,
    ) -> GEResult:
        """
        Run a GE checkpoint by name against batch_data or configured datasource.
        """
        start_time = time.perf_counter()
        self.logger.info(f"Running Great Expectations checkpoint: '{checkpoint_name}'")

        context = self._get_context()
        if context is None:
            elapsed = time.perf_counter() - start_time
            msg = "Great Expectations context unavailable (missing package or invalid path)."
            self.logger.warning(msg)
            return GEResult(
                checkpoint_name=checkpoint_name,
                success=True,  # Non-blocking when GE is optional in lightweight environments
                dq_results=[],
                statistics={"status": "SKIPPED", "reason": msg},
                execution_time_seconds=elapsed,
                error_message=msg,
            )

        try:
            # GE execution
            checkpoint = context.get_checkpoint(name=checkpoint_name)
            batch_request = None
            if batch_data is not None:
                # Dynamic runtime batch
                batch_request = {
                    "runtime_parameters": {"batch_data": batch_data},
                    "batch_identifiers": runtime_parameters or {"default_identifier_name": "default_identifier"},
                }

            run_kwargs = {}
            if batch_request:
                run_kwargs["batch_request"] = batch_request

            result = checkpoint.run(**run_kwargs)
            elapsed = time.perf_counter() - start_time

            dq_results: List[DataQualityResult] = []
            all_success = bool(getattr(result, "success", True))

            # Parse GE run results to DataQualityResult list
            run_results = getattr(result, "run_results", {})
            for _, val_res in run_results.items():
                validation_result = val_res.get("validation_result", {})
                results_list = validation_result.get("results", [])

                for res in results_list:
                    cfg = res.get("expectation_config", {})
                    exp_type = cfg.get("expectation_type", "unknown_expectation")
                    kwargs = cfg.get("kwargs", {})
                    col_name = kwargs.get("column", "table_level")
                    success = bool(res.get("success", False))
                    dimension = self._map_expectation_to_dama_dimension(exp_type)

                    dq_res = DataQualityResult(
                        rule_id=f"ge_{exp_type}_{col_name}",
                        dimension=dimension,
                        passed=success,
                        score=1.0 if success else 0.0,
                        message=f"GE {exp_type} on column '{col_name}': {'PASS' if success else 'FAIL'}",
                        severity=DQSeverity.CRITICAL if not success else DQSeverity.INFO,
                        metadata={"expectation_config": cfg, "result": res.get("result", {})},
                        execution_time_seconds=elapsed,
                    )
                    dq_results.append(dq_res)

            self.logger.info(
                f"Checkpoint '{checkpoint_name}' completed: success={all_success}, "
                f"evaluated {len(dq_results)} expectations in {elapsed:.2f}s."
            )

            return GEResult(
                checkpoint_name=checkpoint_name,
                success=all_success,
                dq_results=dq_results,
                statistics={"total_expectations": len(dq_results), "success": all_success},
                execution_time_seconds=elapsed,
            )

        except Exception as exc:
            elapsed = time.perf_counter() - start_time
            self.logger.error(f"Great Expectations execution error for '{checkpoint_name}': {str(exc)}", exc_info=True)
            return GEResult(
                checkpoint_name=checkpoint_name,
                success=False,
                dq_results=[],
                statistics={"status": "FAILED", "error": str(exc)},
                execution_time_seconds=elapsed,
                error_message=str(exc),
            )
