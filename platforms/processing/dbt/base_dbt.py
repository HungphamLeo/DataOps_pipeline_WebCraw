"""
Module: platforms.processing.dbt.base_dbt
Layer: Platform Processing Subsystem - dbt Engine
Responsibility: Domain-agnostic dbt CLI executor providing structured command invocation,
                model selection, freshness testing, execution timing, and error capture.
Does NOT contain: Domain models, source-specific SQL, direct database driver management.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import subprocess
import time

from shared.logger.python_main_logger import logger_manager


@dataclass
class DbtConfig:
    """
    Configuration specification for dbt execution environment.
    """
    project_dir: str | Path
    profiles_dir: Optional[str | Path] = None
    target: Optional[str] = None
    vars_dict: Dict[str, Any] = field(default_factory=dict)
    threads: Optional[int] = None

    def __post_init__(self):
        self.project_dir = Path(self.project_dir)
        if self.profiles_dir:
            self.profiles_dir = Path(self.profiles_dir)


@dataclass
class DbtResult:
    """
    Immutable representation of a dbt command execution result.
    """
    command: str
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float
    success: bool = True

    def __post_init__(self):
        self.success = (self.returncode == 0)


class DbtRunner:
    """
    Domain-agnostic dbt command executor implementing SOLID (SRP & DIP) principles.
    Executes dbt operations (run, test, source freshness, build) via isolated subprocess.
    """

    def __init__(self, config: DbtConfig):
        self.config = config
        self.logger = logger_manager.get_logger("logger.dbt")

    def _build_base_cmd(self, subcmd: str) -> List[str]:
        cmd = ["dbt", subcmd, "--project-dir", str(self.config.project_dir)]
        if self.config.profiles_dir:
            cmd.extend(["--profiles-dir", str(self.config.profiles_dir)])
        if self.config.target:
            cmd.extend(["--target", self.config.target])
        if self.config.threads:
            cmd.extend(["--threads", str(self.config.threads)])
        if self.config.vars_dict:
            cmd.extend(["--vars", json.dumps(self.config.vars_dict)])
        return cmd

    def _execute(self, cmd: List[str]) -> DbtResult:
        cmd_str = " ".join(cmd)
        self.logger.info(f"Executing dbt command: {cmd_str}")
        start_time = time.perf_counter()

        try:
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )
            elapsed = time.perf_counter() - start_time

            if process.returncode == 0:
                self.logger.info(f"dbt command completed successfully in {elapsed:.2f}s.")
            else:
                self.logger.error(f"dbt command failed with returncode {process.returncode} in {elapsed:.2f}s.\nStderr: {process.stderr}")

            return DbtResult(
                command=cmd_str,
                returncode=process.returncode,
                stdout=process.stdout,
                stderr=process.stderr,
                elapsed_seconds=elapsed,
            )
        except Exception as exc:
            elapsed = time.perf_counter() - start_time
            self.logger.error(f"Unexpected exception during dbt command execution '{cmd_str}': {str(exc)}", exc_info=True)
            return DbtResult(
                command=cmd_str,
                returncode=-1,
                stdout="",
                stderr=str(exc),
                elapsed_seconds=elapsed,
            )

    def run(self, models: Optional[str | List[str]] = None, select: Optional[str] = None) -> DbtResult:
        """
        Execute `dbt run` for specified models or selector.
        """
        cmd = self._build_base_cmd("run")
        if select:
            cmd.extend(["--select", select])
        elif models:
            model_target = " ".join(models) if isinstance(models, list) else models
            cmd.extend(["--select", model_target])
        return self._execute(cmd)

    def test(self, models: Optional[str | List[str]] = None, select: Optional[str] = None) -> DbtResult:
        """
        Execute `dbt test` for data quality validation on dbt models.
        """
        cmd = self._build_base_cmd("test")
        if select:
            cmd.extend(["--select", select])
        elif models:
            model_target = " ".join(models) if isinstance(models, list) else models
            cmd.extend(["--select", model_target])
        return self._execute(cmd)

    def source_freshness(self, select: Optional[str] = None) -> DbtResult:
        """
        Execute `dbt source freshness` to check data ingestion timeliness.
        """
        cmd = self._build_base_cmd("source")
        cmd.append("freshness")
        if select:
            cmd.extend(["--select", select])
        return self._execute(cmd)

    def run_and_test(self, models: Optional[str | List[str]] = None, select: Optional[str] = None) -> DbtResult:
        """
        Sequentially execute `dbt run` followed by `dbt test`.
        If run fails, test is skipped.
        """
        run_res = self.run(models=models, select=select)
        if not run_res.success:
            return run_res

        test_res = self.test(models=models, select=select)
        return DbtResult(
            command=f"{run_res.command} && {test_res.command}",
            returncode=test_res.returncode,
            stdout=f"[RUN STDOUT]\n{run_res.stdout}\n\n[TEST STDOUT]\n{test_res.stdout}",
            stderr=f"[RUN STDERR]\n{run_res.stderr}\n\n[TEST STDERR]\n{test_res.stderr}",
            elapsed_seconds=run_res.elapsed_seconds + test_res.elapsed_seconds,
        )
