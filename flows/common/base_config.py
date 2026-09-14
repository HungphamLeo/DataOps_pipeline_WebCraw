"""
flows/shared/base_config.py
============================
BasePipelineConfig — quản lý tập trung toàn bộ config của một pipeline.

Nguyên tắc thiết kế:
  - SRP: class này CHỈ lo việc đọc env + load YAML. Không chứa Executor, không chứa
    business logic.
  - OCP: pipeline mới subclass BasePipelineConfig và override chỉ những gì khác biệt
    (ví dụ: DEFAULT_SYMBOLS, tên YAML config, required_sections).
  - DIP: Executor/Orchestrator nhận BasePipelineConfig qua constructor, không import
    module-level constants trực tiếp.

Vấn đề được giải quyết so với context.py cũ:
  - context.py cũ: module-level `os.getenv()` chạy ngay lúc import → side effects,
    khó test, khó mock.
  - BasePipelineConfig: lazy load — env chỉ được đọc khi gọi `load()` hoặc truy cập
    property. Safe khi import.
  - os.environ["S3_ENDPOINT"] = ... (global mutation) đã được loại bỏ khỏi import time.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv


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
        from shared.logger.python_main_logger import logger_manager

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
