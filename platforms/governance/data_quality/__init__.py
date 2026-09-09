"""
Package: platforms.governance.data_quality
Layer: Platform Governance Subsystem - Data Quality
Responsibility: Public API export for DAMA-compliant Data Quality Framework,
                including DQ dimensions, rules, rule sets, rule registry, and GE runner.
Does NOT contain: Pipeline domain logic, database operations, flow orchestration.
"""

from platforms.governance.data_quality.dq_dimensions import (
    DQDimension,
    DQSeverity,
    DataQualityResult,
    DataQualityRule,
    DataQualityRuleSet,
)
from platforms.governance.data_quality.dq_registry import (
    RuleRegistry,
    rule_registry,
)
from platforms.governance.data_quality.great_expectation.ge_runner import (
    GreatExpectationsRunner,
    GEResult,
)

__all__ = [
    "DQDimension",
    "DQSeverity",
    "DataQualityResult",
    "DataQualityRule",
    "DataQualityRuleSet",
    "RuleRegistry",
    "rule_registry",
    "GreatExpectationsRunner",
    "GEResult",
]
