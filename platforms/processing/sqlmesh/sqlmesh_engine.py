"""
platforms/processing/sqlmesh/sqlmesh_engine.py
===============================================
Layer: Platform Processing — SQLMesh engine wrapper.
Responsibility: Wrap SQLMesh Context for plan/run/audit operations.
Does NOT contain: model SQL, domain business logic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional


@dataclass
class SqlMeshConfig:
    project_path: str
    gateway: str = "local_duckdb"


class SqlMeshEngine:
    """Thin wrapper around SQLMesh Context for plan/run/audit."""

    def __init__(self, config: SqlMeshConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    def _context(self):
        from sqlmesh import Context
        return Context(paths=[self.config.project_path], gateway=self.config.gateway)

    def plan(self, environment: str = "prod") -> None:
        ctx = self._context()
        plan = ctx.plan(environment=environment, auto_apply=True, no_prompts=True)
        self.logger.info("[SQLMesh] plan applied env=%s", environment)

    def run(self, environment: str = "prod", start: Optional[str] = None, end: Optional[str] = None) -> None:
        ctx = self._context()
        ctx.run(environment=environment, start=start, end=end)
        self.logger.info("[SQLMesh] run complete env=%s", environment)

    def audit(self) -> None:
        ctx = self._context()
        ctx.audit()
        self.logger.info("[SQLMesh] audit complete")
