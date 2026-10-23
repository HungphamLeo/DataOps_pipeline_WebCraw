"""
PostgreSQL Storage Backend
===========================
Refactored with:
  - Connection pooling (psycopg2 ThreadedConnectionPool)
  - Stale connection detection (ping before use, reconnect on failure)
  - Pool metrics logging (background thread, mỗi 60s)
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

import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool

from platforms.storage.base_storage import IRelationalStorage, StorageBackend
from shared.logger.logger_setup import logger_manager


# ---------------------------------------------------------------------------
# Pool Metrics Logger — background thread, log mỗi 60s
# ---------------------------------------------------------------------------

class _PoolMetricsLogger:
    """
    Background daemon thread log pool stats mỗi interval giây.

    Log dạng JSON-structured để ELK/Grafana dễ parse:
        pool_min, pool_max, checked_out, available
    """

    def __init__(self, pool: pg_pool.ThreadedConnectionPool, pool_max: int,
                 logger, interval: int = 60) -> None:
        self._pool     = pool
        self._pool_max = pool_max
        self._logger   = logger
        self._interval = interval
        self._stop     = threading.Event()
        self._thread   = threading.Thread(
            target=self._run, daemon=True, name="pg-pool-metrics"
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                # psycopg2 ThreadedConnectionPool không expose stats trực tiếp,
                # dùng internal _used dict để đếm checked-out connections
                checked_out = len(getattr(self._pool, "_used", {}))
                available   = self._pool_max - checked_out
                self._logger.info(
                    "[PG Pool] pool_max=%d checked_out=%d available=%d",
                    self._pool_max, checked_out, available,
                )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# PostgreSQLWriter
# ---------------------------------------------------------------------------

class PostgreSQLWriter(IRelationalStorage):
    """
    Thread-safe PostgreSQL writer với connection pool + stale detection.

    Parameters
    ----------
    host, port, database, username, password : connection params.
    pool_min, pool_max : connection pool bounds (default 1–5).
    metrics_interval   : giây giữa các lần log pool stats (0 = tắt).
    """

    def __init__(
        self,
        host:             str,
        port:             int,
        database:         str,
        username:         str,
        password:         str,
        pool_min:         int = 1,
        pool_max:         int = 5,
        metrics_interval: int = 60,
        logger=None,
    ) -> None:
        self.host     = host
        self.port     = port
        self.database = database
        self.user     = username
        self.password = password
        self._pool_min = pool_min
        self._pool_max = pool_max
        self.logger   = logger or logger_manager.get_logger(__name__)

        self._conn_kwargs = dict(
            host=host, port=port, database=database,
            user=username, password=password,
        )

        self._pool = pg_pool.ThreadedConnectionPool(
            pool_min, pool_max, **self._conn_kwargs
        )
        self.logger.info(
            "[PG] Pool created pool_min=%d pool_max=%d host=%s db=%s",
            pool_min, pool_max, host, database,
        )

        # Khởi động metrics logger nếu interval > 0
        self._metrics_logger: Optional[_PoolMetricsLogger] = None
        if metrics_interval > 0:
            self._metrics_logger = _PoolMetricsLogger(
                pool=self._pool,
                pool_max=pool_max,
                logger=self.logger,
                interval=metrics_interval,
            )
            self._metrics_logger.start()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _ping(self, conn) -> bool:
        """Kiểm tra connection còn sống không bằng SELECT 1."""
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _fresh_conn(self):
        """Tạo connection mới trực tiếp (bypass pool) khi cần reconnect."""
        return psycopg2.connect(**self._conn_kwargs)

    @contextmanager
    def _conn(self):
        """
        Acquire connection từ pool, kiểm tra stale, reconnect nếu cần.

        Flow:
            1. getconn() từ pool
            2. ping() — nếu fail → putconn(close=True) → tạo connection mới
            3. yield connection
            4. commit hoặc rollback tùy exception
            5. putconn() trả về pool
        """
        conn = self._pool.getconn()
        reconnected = False
        try:
            if not self._ping(conn):
                # Connection cũ bị drop (vd: sau Docker restart PG)
                self.logger.warning("[PG] Stale connection detected — reconnecting")
                try:
                    self._pool.putconn(conn, close=True)
                except Exception:
                    pass
                conn = self._fresh_conn()
                reconnected = True

            yield conn
            conn.commit()

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        finally:
            if reconnected:
                # Connection mới tạo trực tiếp, đóng sau dùng (không trả pool)
                try:
                    conn.close()
                except Exception:
                    pass
            else:
                self._pool.putconn(conn)

    def close(self) -> None:
        """Dừng metrics logger và đóng tất cả connections trong pool."""
        if self._metrics_logger:
            self._metrics_logger.stop()
        self._pool.closeall()
        self.logger.info("[PG] Pool closed")

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
        target:           str,
        data:             List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns:   Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        INSERT ... ON CONFLICT (conflict_columns) DO UPDATE SET ...

        If *update_columns* is None, all non-conflict columns are updated.
        """
        if not data:
            return {"ok": True, "upserted_count": 0}

        keys          = list(data[0].keys())
        cols          = ", ".join(f'"{k}"' for k in keys)
        placeholders  = ", ".join(f"%({k})s" for k in keys)
        conflict_cols = ", ".join(f'"{c}"' for c in conflict_columns)

        upd_cols = update_columns or [k for k in keys if k not in conflict_columns]
        if not upd_cols:
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


# ---------------------------------------------------------------------------
# PostgreSQLStorageBackend — legacy adapter
# ---------------------------------------------------------------------------

class PostgreSQLStorageBackend(StorageBackend):
    """
    Adapter: exposes PostgreSQLWriter as a StorageBackend (legacy interface).

    Callers that already use StorageBackend.save() continue to work unchanged.
    """

    def __init__(
        self,
        pipeline_logger=None,
        postgres_writer: Optional[PostgreSQLWriter] = None,
    ) -> None:
        self.pg     = postgres_writer
        self.logger = pipeline_logger or logger_manager.get_logger(__name__)

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