"""
Pipeline Execution Context + Runtime Constants
==============================================
Tất cả constants đọc từ env var được đặt ở đây — không rải khắp file.
"""
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Runtime constants (all read from env — never hardcoded)
# ---------------------------------------------------------------------------

LAKEHOUSE_BASE: str = os.getenv("LAKEHOUSE_BASE_PATH", "s3://lakehouse")

_S3_ENDPOINT_RAW = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ENDPOINT: str = _S3_ENDPOINT_RAW.split("://")[-1]           # HOST:PORT (no scheme)
S3_ENDPOINT_URL: str = (
    _S3_ENDPOINT_RAW if "://" in _S3_ENDPOINT_RAW else f"http://{_S3_ENDPOINT_RAW}"
)

# Normalise for SQLMesh / DuckDB (they need HOST:PORT without http://)
os.environ["S3_ENDPOINT"] = S3_ENDPOINT

S3_KEY: str = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
S3_SECRET: str = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_123@#"),
)
os.environ.setdefault("AWS_ACCESS_KEY_ID", S3_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", S3_SECRET)

PG_HOST: str     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT: str     = os.getenv("POSTGRES_PORT", "5432")
PG_DB: str       = os.getenv("POSTGRES_DB", "etl_project")
PG_USER: str     = os.getenv("POSTGRES_USER", "")
PG_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

STORAGE_OPTIONS: Dict[str, str] = {
    "endpoint_url":        S3_ENDPOINT_URL,
    "aws_access_key_id":   S3_KEY,
    "aws_secret_access_key": S3_SECRET,
}

DEFAULT_SYMBOLS: List[str] = ["FPT", "VNM", "HPG", "MBB", "SSI"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = (
    PROJECT_ROOT
    / "platforms"
    / "orchestration"
    / "prefect"
    / "config"
    / "cophieu68_config.yaml"
)

SQLMESH_PATH: str = os.getenv(
    "SQLMESH_PATH",
    str(PROJECT_ROOT / "platforms" / "processing" / "sqlmesh"),
)
SQLMESH_GATEWAY: str = os.getenv("SQLMESH_GATEWAY", "local_duckdb")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ExecutionPhase(str, Enum):
    BRONZE  = "bronze"
    SILVER  = "silver"
    GOLD    = "gold"
    SERVING = "serving"
    FULL    = "full"
    VALIDATE = "validate"


class ProcessingBackend(str, Enum):
    POLARS   = "polars"
    SPARK    = "spark"
    SQLMESH  = "sqlmesh"
    DUCKDB   = "duckdb"
    DBT      = "dbt"


# ---------------------------------------------------------------------------
# ID generators
# ---------------------------------------------------------------------------

def make_batch_id(symbol: str) -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"batch_{symbol.upper()}_{ts}_{uid}"


def make_run_id() -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"run_{ts}_{uid}"


# ---------------------------------------------------------------------------
# Execution context dataclass
# ---------------------------------------------------------------------------

@dataclass
class ExecutionContext:
    run_id:        str
    phase:         ExecutionPhase
    symbols:       List[str]
    backend:       ProcessingBackend
    target_date:   str
    environment:   str  = "dev"
    dry_run:       bool = False
    start_time:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time:      Optional[datetime] = None
    status:        str  = "RUNNING"
    error_message: Optional[str]  = None
    metadata_repo: Optional[Any] = None
    error_log:     Optional[Any] = None
    logger:        Optional[logging.Logger] = None

    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()
