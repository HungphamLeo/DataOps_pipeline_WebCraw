"""
Package: platforms.processing.dbt
Layer: Platform Processing Subsystem - dbt Engine
Responsibility: Public API export for domain-agnostic dbt executor and runner configuration.
Does NOT contain: Domain-specific SQL definitions, extraction logic, pipeline orchestration.
"""

from platforms.processing.dbt.base_dbt import (
    DbtConfig,
    DbtResult,
    DbtRunner,
)

__all__ = [
    "DbtConfig",
    "DbtResult",
    "DbtRunner",
]
