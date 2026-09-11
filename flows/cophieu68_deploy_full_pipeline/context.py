"""
flows/cophieu68_deploy_full_pipeline/context.py
================================================
Backward-compatible re-exports.

Sau refactor, các primitives đã chuyển về:
  - ExecutionContext, ExecutionPhase, ProcessingBackend, make_run_id, make_batch_id
    → flows/shared/context.py

  - Tất cả env vars / credentials
    → flows/cophieu68_deploy_full_pipeline/pipeline_config.py (Cophieu68PipelineConfig)

File này giữ lại để các import cũ trong bronze.py / silver.py / gold.py / serving.py
không bị break ngay. Các file đó sẽ dần cập nhật import trực tiếp từ flows.shared.
"""
from __future__ import annotations

# Re-export shared context primitives (không thay đổi interface)
from flows.common.context import (
    ExecutionPhase,
    ProcessingBackend,
    ExecutionContext,
    make_run_id,
    make_batch_id,
)

# Re-export config class (thay thế module-level constants cũ)
from flows.cophieu68_deploy_full_pipeline.pipeline_config import (
    Cophieu68PipelineConfig,
    _DEFAULT_CONFIG_PATH as DEFAULT_CONFIG_PATH,
    _PROJECT_ROOT        as PROJECT_ROOT,
)

# ---------------------------------------------------------------------------
# Backward-compat: các module cũ import LAKEHOUSE_BASE, S3_ENDPOINT, ...
# từ context.py trực tiếp. Dùng PEP 562 module __getattr__ để lazy-load
# từ Cophieu68PipelineConfig — không chạy lúc import.
# ---------------------------------------------------------------------------

_cfg: "Cophieu68PipelineConfig | None" = None

def _ensure_cfg() -> Cophieu68PipelineConfig:
    global _cfg
    if _cfg is None:
        _cfg = Cophieu68PipelineConfig()
        _cfg.load()
    return _cfg

# PEP 562 — module __getattr__ (Python 3.7+)
_LAZY_ATTRS = {
    "LAKEHOUSE_BASE":  lambda c: c.lakehouse_base,
    "S3_ENDPOINT":     lambda c: c.s3_endpoint,
    "S3_ENDPOINT_URL": lambda c: c.s3_endpoint_url,
    "S3_KEY":          lambda c: c.s3_key,
    "S3_SECRET":       lambda c: c.s3_secret,
    "STORAGE_OPTIONS": lambda c: c.storage_options,
    "PG_HOST":         lambda c: c.pg_host,
    "PG_PORT":         lambda c: str(c.pg_port),
    "PG_DB":           lambda c: c.pg_database,
    "PG_USER":         lambda c: c.pg_user,
    "PG_PASSWORD":     lambda c: c.pg_password,
    "DEFAULT_SYMBOLS": lambda c: c.DEFAULT_SYMBOLS,
}


def __getattr__(name: str):
    """
    PEP 562 module __getattr__: trả về lazy constant khi được import.
    Ví dụ: `from flows.cophieu68_deploy_full_pipeline.context import LAKEHOUSE_BASE`
    vẫn hoạt động nhưng giá trị được đọc từ env lúc dùng, không lúc import.
    """
    if name in _LAZY_ATTRS:
        return _LAZY_ATTRS[name](_ensure_cfg())
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
