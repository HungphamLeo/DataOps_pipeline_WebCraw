"""
flows/cophieu68_deploy_full_pipeline/pipeline_config.py
=========================================================
Cophieu68PipelineConfig — config class của pipeline cophieu68.

Subclass của BasePipelineConfig. Chứa:
  - DEFAULT_SYMBOLS    : danh sách mã mặc định (cophieu68-specific)
  - DEFAULT_CONFIG_PATH: đường dẫn đến cophieu68_config.yaml
  - REQUIRED_YAML_SECTIONS: các section bắt buộc trong YAML
  - Validation cụ thể (base_url, sources.cophieu68)
  - Properties lấy ra config từng subsystem (polars, http, sources)
    để truyền thẳng vào builders — không còn truy cập yaml_params tản mát

Không chứa:
  - Env vars module-level (tất cả đều lazy qua properties kế thừa từ BasePipelineConfig)
  - Business logic (bronze/silver/gold)
  - Executor code
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from flows.common.base_config import BasePipelineConfig


# ---------------------------------------------------------------------------
# Path constants (chỉ là đường dẫn, không load gì cả)
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG_PATH = (
    _PROJECT_ROOT
    / "flows"
    / "cophieu68_deploy_full_pipeline"
    / "ingestion"
    / "config"
    / "cophieu68_config.yaml"
)


class Cophieu68PipelineConfig(BasePipelineConfig):
    """
    Config dành riêng cho pipeline cophieu68.

    Usage:
        config = Cophieu68PipelineConfig().load()

        # Truyền vào builders:
        engine  = build_polars_engine(**config.polars_build_params)
        pg      = build_pg_writer(**config.pg_conn_params)
        minio   = build_minio_backend(**config.minio_build_params)
    """

    PIPELINE_NAME           = "cophieu68"
    DEFAULT_CONFIG_PATH     = _DEFAULT_CONFIG_PATH
    REQUIRED_YAML_SECTIONS  = ["sources", "http"]

    # Danh sách mã mặc định — chỉ dùng khi caller không truyền symbols
    DEFAULT_SYMBOLS: List[str] = ["FPT", "VNM", "HPG", "MBB", "SSI"]

    # ------------------------------------------------------------------
    # Domain-specific validation (override hook từ BasePipelineConfig)
    # ------------------------------------------------------------------

    def _validate_extra(
        self, params: Dict[str, Any], errors: List[str]
    ) -> None:
        if not params.get("sources", {}).get("cophieu68", {}).get("base_url"):
            errors.append("Missing project_params.sources.cophieu68.base_url")

    # ------------------------------------------------------------------
    # Properties — builder params (truyền thẳng vào client_factory)
    # ------------------------------------------------------------------

    @property
    def polars_build_params(self) -> Dict[str, Any]:
        """Kwargs truyền thẳng vào build_polars_engine(...)."""
        p = self.get("polars", {})
        return {
            "storage_options":  self.storage_options,
            "thread_pool_size": p.get("thread_pool_size"),
            "enable_streaming": p.get("enable_streaming", True),
        }

    @property
    def minio_build_params(self) -> Dict[str, Any]:
        """Kwargs truyền thẳng vào build_minio_backend(...)."""
        return {
            "endpoint":   self.s3_endpoint,
            "access_key": self.s3_key,
            "secret_key": self.s3_secret,
            "secure":     False,
        }

    # ------------------------------------------------------------------
    # Properties — source config (dùng trong build_extractor)
    # ------------------------------------------------------------------

    @property
    def cophieu68_source_config(self) -> Dict[str, Any]:
        """Config của nguồn dữ liệu cophieu68 từ YAML."""
        return self.get("sources", {}).get("cophieu68", {})

    @property
    def http_config(self) -> Dict[str, Any]:
        """HTTP config (delay, timeout) từ YAML."""
        return self.get("http", {"delay_seconds": 0.5, "timeout_seconds": 30})

    @property
    def extractor_pipeline_config(self) -> Dict[str, Any]:
        """
        Dict truyền vào ExtractCophieu68(pipeline_config=...).
        Format giữ nguyên để không break extractor hiện tại.
        """
        return {
            "sources": {"cophieu68": self.cophieu68_source_config},
            "http":    self.http_config,
        }
