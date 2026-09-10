"""
platforms/factory/client_factory.py
=====================================
Infrastructure client factory — domain-agnostic.

Trách nhiệm (SRP):
  Mỗi function tạo đúng 1 platform client từ credentials được truyền vào.
  Không đọc env vars trực tiếp — credentials do caller cung cấp qua params.
  Không chứa bất kỳ business logic hay pipeline-specific config nào.

Tại sao đặt ở platforms/ không phải flows/shared/:
  Các clients này (Polars, dbt, PostgreSQL, MinIO) là infrastructure concerns.
  Chúng không phụ thuộc vào domain pipeline nào. flows/ chỉ consume, không own.

Usage:
    from platforms.factory.client_factory import build_polars_engine, build_pg_writer

    engine = build_polars_engine(storage_options={...}, thread_pool_size=4)
    pg     = build_pg_writer(host="localhost", port=5432, ...)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# PolarsEngine — đọc/ghi Parquet S3/MinIO
# ---------------------------------------------------------------------------

def build_polars_engine(
    storage_options: Dict[str, str],
    thread_pool_size: Optional[int] = None,
    enable_streaming: bool = True,
    logger: Optional[logging.Logger] = None,
):
    """
    Khởi tạo PolarsEngine với S3 storage options.

    Parameters
    ----------
    storage_options  : Dict với keys endpoint_url, aws_access_key_id, aws_secret_access_key.
    thread_pool_size : Số thread pool (None = dùng default của Polars).
    enable_streaming : Bật streaming mode cho large datasets.
    logger           : Logger instance; nếu None tạo mới.
    """
    from platforms.processing.polars.polars_engine import PolarsConfig, PolarsEngine
    from shared.logger.python_main_logger import logger_manager

    cfg = PolarsConfig(
        storage_options=storage_options,
        thread_pool_size=thread_pool_size,
        enable_streaming=enable_streaming,
    )
    return PolarsEngine(
        config=cfg,
        logger=logger or logger_manager.get_logger("polars_engine"),
    )


# ---------------------------------------------------------------------------
# DbtRunner — chạy dbt CLI (silver transform + gold mart models)
# ---------------------------------------------------------------------------

def build_dbt_runner(
    project_dir: "str | Path | None" = None,
    profiles_dir: "str | Path | None" = None,
    target: Optional[str] = None,
    vars_dict: Optional[Dict[str, Any]] = None,
    threads: Optional[int] = None,
):
    """
    Khởi tạo DbtRunner.

    Parameters
    ----------
    project_dir  : Path đến dbt project. Mặc định <repo_root>/dbt_project.
    profiles_dir : Path đến profiles.yml. None = dbt dùng ~/.dbt/profiles.yml.
    target       : dbt target environment (dev / prod).
    vars_dict    : Dict variables truyền vào dbt (--vars).
    threads      : Override số threads (--threads).
    """
    from platforms.processing.dbt.base_dbt import DbtConfig, DbtRunner

    if project_dir is None:
        # Default: <repo_root>/dbt_project
        project_dir = Path(__file__).parents[2] / "dbt_project"

    cfg = DbtConfig(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        vars_dict=vars_dict or {},
        threads=threads,
    )
    return DbtRunner(config=cfg)


# ---------------------------------------------------------------------------
# PostgreSQLWriter — connection pool, insert/upsert/query
# ---------------------------------------------------------------------------

def build_pg_writer(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    pool_min: int = 1,
    pool_max: int = 5,
    logger: Optional[logging.Logger] = None,
):
    """
    Khởi tạo PostgreSQLWriter với connection pool.
    Trả về None nếu username rỗng (credentials chưa được set).
    """
    from platforms.storage.postgre.base_postgre import PostgreSQLWriter
    from shared.logger.python_main_logger import logger_manager

    if not username:
        return None

    return PostgreSQLWriter(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        pool_min=pool_min,
        pool_max=pool_max,
        logger=logger or logger_manager.get_logger("pg_writer"),
    )


# ---------------------------------------------------------------------------
# MinioStorageBackend — upload/download bytes, read/write Parquet
# ---------------------------------------------------------------------------

def build_minio_backend(
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool = False,
    logger: Optional[logging.Logger] = None,
):
    """
    Khởi tạo MinioStorageBackend.

    Parameters
    ----------
    endpoint   : HOST:PORT (không có http://) — vd: "localhost:9000".
    access_key : MinIO / S3 access key.
    secret_key : MinIO / S3 secret key.
    secure     : True để dùng TLS (False cho local dev).
    """
    from platforms.storage.minio.minio_storage import MinioStorageBackend
    from shared.logger.python_main_logger import logger_manager

    return MinioStorageBackend(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        logger=logger or logger_manager.get_logger("minio_storage"),
    )
