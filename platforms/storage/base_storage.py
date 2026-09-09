"""
Abstract base interfaces for all storage backends.

Follows Interface Segregation Principle (ISP):
  - IObjectStorage   → object-store operations (MinIO/S3)
  - IRelationalStorage → relational DB operations (PostgreSQL)
  - IStorageBackend  → unified read/write abstraction (legacy compat)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class IObjectStorage(ABC):
    """Abstraction for object storage (MinIO / S3-compatible)."""

    @abstractmethod
    def upload_bytes(self, bucket: str, key: str, data: bytes,
                     content_type: str = "application/octet-stream") -> None: ...

    @abstractmethod
    def download_bytes(self, bucket: str, key: str) -> bytes: ...

    @abstractmethod
    def list_objects(self, bucket: str, prefix: str) -> List[str]: ...

    @abstractmethod
    def object_exists(self, bucket: str, key: str) -> bool: ...

    @abstractmethod
    def ensure_bucket(self, bucket: str) -> None: ...


class IRelationalStorage(ABC):
    """Abstraction for relational database operations (PostgreSQL)."""

    @abstractmethod
    def execute(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]: ...

    @abstractmethod
    def query(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]: ...

    @abstractmethod
    def insert(self, target: str, data: Any) -> Dict[str, Any]: ...

    @abstractmethod
    def upsert(self, target: str, data: List[Dict[str, Any]],
               conflict_columns: List[str]) -> Dict[str, Any]: ...

    @abstractmethod
    def bulk_insert(self, table: str, data: Any) -> Dict[str, Any]: ...


class StorageBackend(ABC):
    """Legacy unified storage backend (kept for backward compatibility)."""

    @abstractmethod
    def save(self, dataset_name: str, data: Any,
             fmt: Optional[str] = None) -> Dict[str, Any]: ...
