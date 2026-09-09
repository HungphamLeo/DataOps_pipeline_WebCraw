"""
Module: platforms.governance.data_quality.dq_dimensions
Layer: Platform Governance Subsystem - Data Quality
Responsibility: DAMA Data Quality dimensions (Completeness, Consistency, Accuracy,
                Timeliness, Uniqueness, Validity), Rule definitions, RuleSet aggregation,
                and validation execution engine.
Does NOT contain: Flow orchestration, pipeline-specific transformation, storage adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import time

from shared.logger.python_main_logger import logger_manager


class DQDimension(str, Enum):
    """
    6 Core Data Quality Dimensions standardized by DAMA International (DAMA-DMBOK).
    """
    COMPLETENESS = "COMPLETENESS"   # Proportion of stored data against the potential 100%
    CONSISTENCY  = "CONSISTENCY"    # Data across multiple data sets matches without conflict
    ACCURACY     = "ACCURACY"       # Degree to which data correctly describes the "real world"
    TIMELINESS   = "TIMELINESS"     # Degree to which data represent reality from the required point in time
    UNIQUENESS   = "UNIQUENESS"     # Things are recorded once; no duplicate values
    VALIDITY     = "VALIDITY"       # Data conforms to the syntax (format, type, range) of its definition


class DQSeverity(str, Enum):
    """
    Severity level for Data Quality validation issues.
    """
    CRITICAL = "CRITICAL"  # Pipeline must abort or quarantine data
    WARNING  = "WARNING"   # Pipeline continues, but alerts are dispatched
    INFO     = "INFO"      # Metrics tracked for observability


@dataclass
class DataQualityResult:
    """
    Result of evaluating a single Data Quality rule or suite.
    """
    rule_id: str
    dimension: DQDimension
    passed: bool
    score: float = 1.0  # Normalized quality score: 0.0 to 1.0
    message: str = ""
    severity: DQSeverity = DQSeverity.CRITICAL
    metadata: Dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "dimension": self.dimension.value,
            "passed": self.passed,
            "score": self.score,
            "message": self.message,
            "severity": self.severity.value,
            "metadata": self.metadata,
            "execution_time_seconds": self.execution_time_seconds,
        }


@dataclass
class DataQualityRule:
    """
    Atomic Data Quality Rule adhering to DAMA standards.
    check_fn signature: Callable[[Any], tuple[bool, float, str] | bool]
    """
    rule_id: str
    dimension: DQDimension
    description: str
    check_fn: Callable[[Any], Any]
    severity: DQSeverity = DQSeverity.CRITICAL
    target_column: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)

    def evaluate(self, dataset: Any) -> DataQualityResult:
        """
        Execute check_fn against the given dataset (Polars/Pandas DataFrame, Dict, List, etc.).
        """
        logger = logger_manager.get_logger("logger.data_quality")
        start_time = time.perf_counter()
        try:
            res = self.check_fn(dataset)
            elapsed = time.perf_counter() - start_time

            if isinstance(res, tuple):
                # Format: (passed: bool, score: float, message: str) or (passed: bool, message: str)
                if len(res) == 3:
                    passed, score, message = res
                elif len(res) == 2:
                    passed, message = res
                    score = 1.0 if passed else 0.0
                else:
                    passed = bool(res[0])
                    score = 1.0 if passed else 0.0
                    message = ""
            elif isinstance(res, bool):
                passed = res
                score = 1.0 if passed else 0.0
                message = "Passed validation" if passed else f"Rule {self.rule_id} failed"
            elif isinstance(res, DataQualityResult):
                res.execution_time_seconds = elapsed
                return res
            else:
                passed = bool(res)
                score = 1.0 if passed else 0.0
                message = str(res)

            return DataQualityResult(
                rule_id=self.rule_id,
                dimension=self.dimension,
                passed=bool(passed),
                score=float(score),
                message=str(message),
                severity=self.severity,
                metadata={"target_column": self.target_column, "params": self.params},
                execution_time_seconds=elapsed,
            )
        except Exception as exc:
            elapsed = time.perf_counter() - start_time
            logger.error(f"Rule evaluation error on {self.rule_id}: {str(exc)}", exc_info=True)
            return DataQualityResult(
                rule_id=self.rule_id,
                dimension=self.dimension,
                passed=False,
                score=0.0,
                message=f"Evaluation raised error: {str(exc)}",
                severity=self.severity,
                metadata={"error": str(exc), "target_column": self.target_column},
                execution_time_seconds=elapsed,
            )


@dataclass
class DataQualityRuleSet:
    """
    Collection of DataQualityRules that can be evaluated atomically or as a batch.
    """
    name: str
    rules: List[DataQualityRule] = field(default_factory=list)

    def add_rule(self, rule: DataQualityRule) -> DataQualityRuleSet:
        self.rules.append(rule)
        return self

    def apply_all(self, dataset: Any) -> List[DataQualityResult]:
        """
        Evaluate all registered rules in this ruleset against dataset.
        """
        logger = logger_manager.get_logger("logger.data_quality")
        logger.info(f"Starting Data Quality evaluation for ruleset: {self.name} ({len(self.rules)} rules)")

        results: List[DataQualityResult] = []
        for rule in self.rules:
            res = rule.evaluate(dataset)
            results.append(res)
            if not res.passed:
                log_msg = f"[DQ {res.dimension.value}] [{res.severity.value}] {res.rule_id}: {res.message}"
                if res.severity == DQSeverity.CRITICAL:
                    logger.error(log_msg)
                elif res.severity == DQSeverity.WARNING:
                    logger.warning(log_msg)
                else:
                    logger.info(log_msg)

        passed_cnt = sum(1 for r in results if r.passed)
        logger.info(f"Ruleset {self.name} completed: {passed_cnt}/{len(results)} rules passed.")
        return results

    @property
    def is_all_passed(self) -> bool:
        # Note: Must be checked after apply_all results
        return False
