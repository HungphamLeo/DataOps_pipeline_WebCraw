"""
MinIO Storage Backend
=====================
Concrete implementation of IObjectStorage for MinIO / S3-compatible stores.

Responsibilities (SRP):
  - Upload / download bytes (parquet files, JSON, etc.)
  - Ensure bucket exists
  - List objects under a prefix
  - Write a Polars DataFrame as Parquet directly to MinIO

NOT responsible for:
  - Data transformation (that belongs to processing layer)
  - Connection pooling of DB (separate concern)
  - Diagnostic reporting (use probe scripts separately)
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import List, Optional

from platforms.storage.base_storage import IObjectStorage


class MinioStorageBackend(IObjectStorage):
    """
    MinIO / S3-compatible object storage backend.

    Depends on `minio` package (already in requirements.txt).
    Credentials are injected via constructor — never read from env here.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Parameters
        ----------
        endpoint   : HOST:PORT — e.g. "localhost:9000".  Do NOT include http://.
        access_key : MinIO / S3 access key.
        secret_key : MinIO / S3 secret key.
        secure     : Use TLS (False for local dev).
        """
        from minio import Minio

        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # IObjectStorage implementation
    # ------------------------------------------------------------------

    def ensure_bucket(self, bucket: str) -> None:
        """Create bucket if it does not exist."""
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            self.logger.info("[MinIO] Created bucket: %s", bucket)

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload raw bytes to MinIO."""
        self.ensure_bucket(bucket)
        buf = io.BytesIO(data)
        self._client.put_object(
            bucket_name=bucket,
            object_name=key,
            data=buf,
            length=len(data),
            content_type=content_type,
        )
        self.logger.debug("[MinIO] Uploaded %s / %s (%d bytes)", bucket, key, len(data))

    def download_bytes(self, bucket: str, key: str) -> bytes:
        """Download object content as bytes."""
        response = self._client.get_object(bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, bucket: str, prefix: str) -> List[str]:
        """Return list of object keys under *prefix* (recursive)."""
        objects = self._client.list_objects(bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return True if the object exists in the bucket."""
        from minio.error import S3Error

        try:
            self._client.stat_object(bucket, key)
            return True
        except S3Error:
            return False

    # ------------------------------------------------------------------
    # Polars / Parquet helpers
    # ------------------------------------------------------------------

    def write_dataframe_parquet(
        self,
        df: "polars.DataFrame",  # type: ignore[name-defined]
        bucket: str,
        key: str,
    ) -> str:
        """
        Serialise a Polars DataFrame to Parquet and upload to MinIO.

        Returns the full object path: ``{bucket}/{key}``
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        buf = io.BytesIO()
        pq.write_table(df.to_arrow(), buf)
        buf.seek(0)
        raw = buf.read()
        self.upload_bytes(bucket, key, raw, content_type="application/parquet")
        path = f"{bucket}/{key}"
        self.logger.info("[MinIO] Wrote parquet %s (%d bytes)", path, len(raw))
        return path

    def read_dataframe_parquet(
        self,
        bucket: str,
        key: str,
    ) -> "polars.DataFrame":  # type: ignore[name-defined]
        """Download a Parquet file from MinIO and return a Polars DataFrame."""
        import polars as pl

        raw = self.download_bytes(bucket, key)
        return pl.read_parquet(io.BytesIO(raw))
