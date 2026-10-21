"""
flows/common/base_config.py
============================
BasePipelineConfig  — đọc .env + load YAML. Không chứa business logic.
BaseExecutor        — abstract base cho tất cả phase executors.
BasePipelineOrchestrator — template orchestrator, subclass override _create_executors().
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from shared.logger.logger_setup import logger_manager
import json
from datetime import date, datetime, timezone


class BasePipelineConfig:
    """
    Base class cho tất cả pipeline config.

    Subclass override:
      - PIPELINE_NAME        : str  — tên pipeline, dùng trong log
      - DEFAULT_CONFIG_PATH  : Path — path mặc định đến YAML config
      - REQUIRED_YAML_SECTIONS: List[str] — sections phải có trong YAML

    Usage:
        config = Cophieu68PipelineConfig()
        config.load()               # đọc .env + YAML
        engine = build_polars_engine(storage_options=config.storage_options)
        pg     = build_pg_writer(**config.pg_conn_params)
    """

    PIPELINE_NAME:           str        = "base"
    DEFAULT_CONFIG_PATH:     Path       = Path("config.yaml")
    REQUIRED_YAML_SECTIONS:  List[str]  = []

    def __init__(
        self,
        config_path: Optional[str] = None,
        env_file: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        

        self.logger      = logger or logger_manager.get_logger(
            f"config.{self.PIPELINE_NAME}"
        )
        self._env_file   = env_file
        self._config_path = Path(config_path) if config_path else self.DEFAULT_CONFIG_PATH
        self._yaml_data: Dict[str, Any] = {}
        self._loaded    = False

    # ------------------------------------------------------------------
    # Public: load
    # ------------------------------------------------------------------

    def load(self) -> "BasePipelineConfig":
        """
        Đọc .env file (nếu có) rồi load YAML config.
        Idempotent — gọi nhiều lần không có side effect.
        """
        if self._loaded:
            return self
        load_dotenv(self._env_file, override=False)
        self._normalize_s3_env()
        self._yaml_data = self._load_yaml(self._config_path)
        self._loaded = True
        self.logger.info(
            "[Config:%s] Loaded from %s", self.PIPELINE_NAME, self._config_path
        )
        return self

    def validate(self) -> Dict[str, Any]:
        """
        Kiểm tra các sections bắt buộc trong YAML config.
        Subclass override để thêm domain-specific validations.
        """
        errors: List[str] = []
        params = self.yaml_params
        for section in self.REQUIRED_YAML_SECTIONS:
            if section not in params:
                errors.append(f"Missing section: project_params.{section}")
        self._validate_extra(params, errors)
        return {"is_valid": not errors, "errors": errors}

    def _validate_extra(
        self, params: Dict[str, Any], errors: List[str]
    ) -> None:
        """Hook cho subclass thêm validation logic."""

    # ------------------------------------------------------------------
    # Properties — Infrastructure credentials
    # ------------------------------------------------------------------

    @property
    def s3_endpoint_raw(self) -> str:
        return os.getenv("S3_ENDPOINT", "http://localhost:9000")

    @property
    def s3_endpoint(self) -> str:
        """HOST:PORT không có scheme — dùng cho MinIO client."""
        raw = self.s3_endpoint_raw
        return raw.split("://")[-1]

    @property
    def s3_endpoint_url(self) -> str:
        """Full URL với scheme — dùng cho storage_options."""
        raw = self.s3_endpoint_raw
        return raw if "://" in raw else f"http://{raw}"

    @property
    def s3_key(self) -> str:
        return os.getenv(
            "AWS_ACCESS_KEY_ID",
            os.getenv("MINIO_ROOT_USER", "minioadmin"),
        )

    @property
    def s3_secret(self) -> str:
        return os.getenv(
            "AWS_SECRET_ACCESS_KEY",
            os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_123@#"),
        )

    @property
    def lakehouse_base(self) -> str:
        return os.getenv("LAKEHOUSE_BASE_PATH", "s3://dataops-lake")

    @property
    def storage_options(self) -> Dict[str, str]:
        """Dict truyền thẳng vào build_polars_engine(storage_options=...)."""
        return {
            "endpoint_url":          self.s3_endpoint_url,
            "aws_access_key_id":     self.s3_key,
            "aws_secret_access_key": self.s3_secret,
        }

    @property
    def polars_build_params(self) -> Dict[str, Any]:
        """Kwargs cho build_polars_engine()."""
        return {"storage_options": self.storage_options}

    @property
    def extractor_pipeline_config(self) -> Dict[str, Any]:
        """Config dict truyền thẳng vào ExtractCophieu68(pipeline_config=...)."""
        return self._yaml_data

    # DEFAULT_SYMBOLS — subclass override để cung cấp mã mặc định
    DEFAULT_SYMBOLS: List[str] = []

    @property
    def pg_host(self) -> str:
        return os.getenv("POSTGRES_HOST", "localhost")

    @property
    def pg_port(self) -> int:
        return int(os.getenv("POSTGRES_PORT", "5432"))

    @property
    def pg_database(self) -> str:
        # Default khớp với POSTGRES_DB trong infra/docker_compose.yml
        return os.getenv("POSTGRES_DB", "dataops_webcraw")

    @property
    def pg_user(self) -> str:
        # Default khớp với POSTGRES_USER trong infra/docker_compose.yml
        return os.getenv("POSTGRES_USER", "postgres@user")

    @property
    def pg_password(self) -> str:
        # Default khớp với POSTGRES_PASSWORD trong infra/docker_compose.yml
        return os.getenv("POSTGRES_PASSWORD", "password@123")

    @property
    def pg_conn_params(self) -> Dict[str, Any]:
        """Dict truyền thẳng vào build_pg_writer(**config.pg_conn_params)."""
        return {
            "host":     self.pg_host,
            "port":     self.pg_port,
            "database": self.pg_database,
            "username": self.pg_user,
            "password": self.pg_password,
        }

    # ------------------------------------------------------------------
    # Properties — YAML data access
    # ------------------------------------------------------------------

    @property
    def yaml_params(self) -> Dict[str, Any]:
        """Unwrap project_params wrapper nếu có."""
        return self._yaml_data.get("project_params", self._yaml_data)

    @property
    def raw_yaml(self) -> Dict[str, Any]:
        return self._yaml_data

    def get(self, key: str, default: Any = None) -> Any:
        """Shorthand truy cập yaml_params[key]."""
        return self.yaml_params.get(key, default)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data
        except FileNotFoundError:
            self.logger.warning("[Config:%s] YAML not found: %s", self.PIPELINE_NAME, path)
            return {}
        except Exception as exc:
            self.logger.error("[Config:%s] YAML load error: %s", self.PIPELINE_NAME, exc)
            return {}

    def _normalize_s3_env(self) -> None:
        """
        Đảm bảo AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY được set trong os.environ
        để các thư viện con (boto3, fsspec, s3fs) tự pick up.
        Ghi chú: đây là side effect CÓ CHỦ ĐÍCH, được gọi explicit qua load()
                 chứ không phải lúc import.
        """
        os.environ.setdefault("AWS_ACCESS_KEY_ID", self.s3_key)
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", self.s3_secret)
        # DuckDB / SQLMesh cần HOST:PORT không có scheme
        os.environ["S3_ENDPOINT"] = self.s3_endpoint


class BaseExecutor(ABC):
    """Abstract base cho tất cả phase executors (Bronze / Silver / Serving)."""

    def __init__(self, context: Any, config: Any) -> None:
        self.context = context
        self.config  = config
        self.logger  = getattr(context, "logger", None) or logger_manager.get_logger(__name__)

    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """Chạy phase, trả về dict có ít nhất {"phase": str, "errors": int}."""

    def pre_execute(self) -> None:
        """Hook trước execute(). Override nếu cần setup."""

    def post_execute(self, result: Dict[str, Any]) -> None:
        """Hook sau execute(). Override nếu cần cleanup."""

    def _log_lineage(
        self,
        run_id: str,
        source_layer: str,
        source_table: str,
        target_layer: str,
        target_table: str,
        operation: str,
        rows: int,
    ) -> None:
        repo = getattr(self.context, "metadata_repo", None)
        if repo is None:
            return
        try:
            repo.log_lineage(
                run_id=run_id,
                source_layer=source_layer,
                source_table=source_table,
                target_layer=target_layer,
                target_table=target_table,
                operation=operation,
                rows_affected=rows,
            )
        except Exception:
            if self.logger:
                self.logger.warning(
                    "[Executor:%s] lineage log failed: %s -> %s",
                    self.__class__.__name__, source_table, target_table,
                )



class BasePipelineOrchestrator:
    """
    Template orchestrator. Subclass override _create_executors() và _phase_order().
    """

    PIPELINE_NAME: str = "base"

    def __init__(
        self,
        config: BasePipelineConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
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
        """Entry point duy nhất. phase: bronze | silver | full | validate"""
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
        """Thứ tự chạy khi phase == FULL. Subclass override nếu cần."""
        return ["bronze", "silver", "serving"]

    def _default_symbols(self) -> List[str]:
        return []

    def _validate_config(self, context: Any) -> Dict[str, Any]:
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
            "status": "COMPLETED",
            "phases_executed": [],
            "phase_results": {},
        }
        for phase_name in phases:
            executor = executors.get(phase_name)
            if executor is None:
                self.logger.warning("[%s] No executor for phase=%s — skipping",
                                    self.PIPELINE_NAME, phase_name)
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


def format_pipeline_result(result: Dict[str, Any], fmt: str = "summary") -> str:
    """Format kết quả pipeline. fmt: 'summary' | 'text' | 'json'"""
    if fmt == "json":
        return json.dumps(result, indent=2, default=str)
    if fmt == "text":
        lines = [
            "=" * 60,
            f"Run ID  : {result.get('run_id', 'N/A')}",
            f"Status  : {result.get('status', 'UNKNOWN')}",
            f"Duration: {result.get('duration_seconds', 0):.1f}s",
        ]
        if result.get("phases_executed"):
            lines.append(f"Phases  : {', '.join(result['phases_executed'])}")
        for ev in result.get("error_events", [])[:5]:
            lines.append(f"  [{ev.get('error_level')}] {ev.get('error_message')}")
        lines.append("=" * 60)
        return "\n".join(lines)
    status = result.get("status", "UNKNOWN")
    dur    = result.get("duration_seconds", 0)
    return f"{status} | {dur:.1f}s"
