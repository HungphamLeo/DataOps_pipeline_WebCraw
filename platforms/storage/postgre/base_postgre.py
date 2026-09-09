"""
PostgreSQL Storage Backend
===========================
Refactored with:
  - Connection pooling (psycopg2 ThreadedConnectionPool)
  - Proper upsert (INSERT ... ON CONFLICT DO UPDATE)
  - Staging schema support
  - IRelationalStorage interface compliance
  - No print() calls — structured logging only

SRP: only handles DB I/O, no business logic.
OCP: extend via subclassing, not by modifying this class.
LSP: PostgreSQLStorageBackend is a drop-in replacement for StorageBackend.
ISP: IRelationalStorage + StorageBackend are separate interfaces.
DIP: Callers depend on IRelationalStorage abstraction, not this concrete class.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool as pg_pool

from platforms.storage.base_storage import IRelationalStorage, StorageBackend


class PostgreSQLWriter(IRelationalStorage):
    """
    Thread-safe PostgreSQL writer with connection pooling.

    Parameters
    ----------
    host, port, database, username, password : connection params.
    pool_min, pool_max : connection pool bounds (default 1–5).
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        pool_min: int = 1,
        pool_max: int = 5,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = username
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self._pool = pg_pool.ThreadedConnectionPool(
            pool_min,
            pool_max,
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
        )

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    @contextmanager
    def _conn(self):
        """Acquire a connection from the pool, release on exit."""
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        """Close all connections in the pool."""
        self._pool.closeall()

    # ------------------------------------------------------------------
    # IRelationalStorage implementation
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """Execute a DDL / DML statement."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
            return {"ok": True}
        except Exception as exc:
            self.logger.error("[PG] execute failed: %s", exc)
            return {"ok": False, "error": str(exc)}

    def query(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """Execute a SELECT and return results."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    cols = [d[0] for d in cur.description]
                    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            return {"ok": True, "results": rows}
        except Exception as exc:
            self.logger.error("[PG] query failed: %s", exc)
            return {"ok": False, "error": str(exc), "results": []}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        """Plain INSERT (no conflict handling)."""
        if not data:
            return {"ok": True, "inserted_count": 0}
        if not isinstance(data, list):
            data = [data]
        keys = list(data[0].keys())
        cols = ", ".join(f'"{k}"' for k in keys)
        placeholders = ", ".join(f"%({k})s" for k in keys)
        sql = f'INSERT INTO {target} ({cols}) VALUES ({placeholders})'
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, data)
            self.logger.info("[PG] Inserted %d rows into %s", len(data), target)
            return {"ok": True, "inserted_count": len(data)}
        except Exception as exc:
            self.logger.error("[PG] insert into %s failed: %s", target, exc)
            return {"ok": False, "inserted_count": 0, "error": str(exc)}

    def upsert(
        self,
        target: str,
        data: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        INSERT ... ON CONFLICT (conflict_columns) DO UPDATE SET ...

        If *update_columns* is None, all non-conflict columns are updated.
        """
        if not data:
            return {"ok": True, "upserted_count": 0}

        keys = list(data[0].keys())
        cols = ", ".join(f'"{k}"' for k in keys)
        placeholders = ", ".join(f"%({k})s" for k in keys)
        conflict_cols = ", ".join(f'"{c}"' for c in conflict_columns)

        upd_cols = update_columns or [k for k in keys if k not in conflict_columns]
        if not upd_cols:
            # All columns are conflict keys — just skip on conflict
            do_clause = "DO NOTHING"
        else:
            set_pairs = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in upd_cols)
            do_clause = f"DO UPDATE SET {set_pairs}"

        sql = (
            f'INSERT INTO {target} ({cols}) VALUES ({placeholders}) '
            f'ON CONFLICT ({conflict_cols}) {do_clause}'
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, data)
            self.logger.info("[PG] Upserted %d rows into %s", len(data), target)
            return {"ok": True, "upserted_count": len(data)}
        except Exception as exc:
            self.logger.error("[PG] upsert into %s failed: %s", target, exc)
            return {"ok": False, "upserted_count": 0, "error": str(exc)}

    def bulk_insert(self, table: str, data: Any) -> Dict[str, Any]:
        """Accept DataFrame or list-of-dicts and INSERT."""
        try:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                data = data.to_dict("records")
        except ImportError:
            pass
        return self.insert(table, data)

    # ------------------------------------------------------------------
    # Schema management helpers
    # ------------------------------------------------------------------

    def ensure_schema(self, schema: str) -> Dict[str, Any]:
        """CREATE SCHEMA IF NOT EXISTS."""
        return self.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    def execute_script(self, sql_script: str) -> Dict[str, Any]:
        """Execute a multi-statement SQL script (separated by ';')."""
        statements = [s.strip() for s in sql_script.split(";") if s.strip()]
        errors = []
        for stmt in statements:
            res = self.execute(stmt)
            if not res.get("ok"):
                errors.append(res.get("error", "unknown"))
        if errors:
            return {"ok": False, "errors": errors}
        return {"ok": True, "statements_executed": len(statements)}


class PostgreSQLStorageBackend(StorageBackend):
    """
    Adapter: exposes PostgreSQLWriter as a StorageBackend (legacy interface).

    Callers that already use StorageBackend.save() continue to work unchanged.
    """

    def __init__(
        self,
        pipeline_logger: Optional[logging.Logger],
        postgres_writer: PostgreSQLWriter,
    ) -> None:
        self.pg = postgres_writer
        self.logger = pipeline_logger or logging.getLogger(__name__)

    def save(self, dataset_name: str, data: Any,
             fmt: Optional[str] = None) -> Dict[str, Any]:
        """Save data to a PostgreSQL table."""
        try:
            if not isinstance(data, list):
                data = [data]
            result = self.pg.insert(dataset_name, data)
            return {"ok": True, **result}
        except Exception as exc:
            self.logger.exception("[PG] StorageBackend.save failed")
            return {"ok": False, "error": str(exc)}
