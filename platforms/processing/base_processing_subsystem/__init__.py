"""
platforms/processing/base_processing_subsystem/
================================================
Layer: Platform Processing — Core processing primitives.
Responsibility: Self-contained error model, metadata repo, deduplication,
                surrogate key generation. Domain-agnostic.
Does NOT contain: crawl logic, MinIO/PG I/O, Polars/DuckDB engines.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Error / Event model  (subsystem 5 + 30)
# ─────────────────────────────────────────────────────────────────────────────

class ErrorLevel(str, Enum):
    INFO    = "INFO"
    WARNING = "WARNING"
    ERROR   = "ERROR"
    FATAL   = "FATAL"


@dataclass
class ErrorEvent:
    error_id:      str
    run_id:        str
    job_name:      str
    error_level:   ErrorLevel
    error_message: str
    timestamp:     str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_id":      self.error_id,
            "run_id":        self.run_id,
            "job_name":      self.job_name,
            "error_level":   self.error_level.value,
            "error_message": self.error_message,
            "timestamp":     self.timestamp,
        }


class ErrorEventLog:
    """Collect error events for a pipeline run."""

    def __init__(self, run_id: str, job_name: str) -> None:
        self.run_id   = run_id
        self.job_name = job_name
        self.events:  List[ErrorEvent] = []

    def add(self, level: ErrorLevel, message: str) -> None:
        self.events.append(ErrorEvent(
            error_id=uuid.uuid4().hex[:8],
            run_id=self.run_id,
            job_name=self.job_name,
            error_level=level,
            error_message=message,
        ))

    def summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for e in self.events:
            counts[e.error_level.value] = counts.get(e.error_level.value, 0) + 1
        return counts

    def has_fatal(self) -> bool:
        return any(e.error_level == ErrorLevel.FATAL for e in self.events)


# ─────────────────────────────────────────────────────────────────────────────
# Metadata repository  (subsystem 34)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _RunRecord:
    job_name:    str
    layer:       str
    table_name:  str
    run_id:      str
    status:      str = "RUNNING"
    started_at:  str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ended_at:    Optional[str] = None
    rows_written: Optional[int] = None
    error_message: Optional[str] = None


class MetadataRepository:
    """In-memory metadata store for pipeline run tracking and data lineage."""

    def __init__(
        self,
        delta_backend: Any = None,   # reserved for future Delta Lake backend
        logger: Optional[logging.Logger] = None,
        in_memory: bool = True,
    ) -> None:
        self._runs:    Dict[str, _RunRecord] = {}
        self._lineage: List[Dict[str, Any]]  = []
        self.logger    = logger or logging.getLogger(__name__)

    def start_run(self, job_name: str, layer: str, table_name: str, run_id: str) -> None:
        self._runs[run_id] = _RunRecord(
            job_name=job_name, layer=layer, table_name=table_name, run_id=run_id
        )
        self.logger.debug("[Meta] start_run run_id=%s job=%s", run_id, job_name)

    def end_run(
        self,
        run_id: str,
        status: str = "SUCCESS",
        rows_written: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        rec = self._runs.get(run_id)
        if rec:
            rec.status        = status
            rec.ended_at      = datetime.now(timezone.utc).isoformat()
            rec.rows_written  = rows_written
            rec.error_message = error_message
        self.logger.debug("[Meta] end_run run_id=%s status=%s", run_id, status)

    def log_lineage(
        self,
        run_id: str,
        source_layer: str,
        source_table: str,
        target_layer: str,
        target_table: str,
        operation: str,
        rows_affected: int,
    ) -> None:
        self._lineage.append({
            "run_id":        run_id,
            "source_layer":  source_layer,
            "source_table":  source_table,
            "target_layer":  target_layer,
            "target_table":  target_table,
            "operation":     operation,
            "rows_affected": rows_affected,
            "recorded_at":   datetime.now(timezone.utc).isoformat(),
        })

    def log_run_summary(self, run_id: str) -> None:
        rec = self._runs.get(run_id)
        if rec:
            self.logger.info(
                "[Meta] run_id=%s job=%s status=%s rows=%s",
                run_id, rec.job_name, rec.status, rec.rows_written,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Surrogate key generator  (subsystem 10)
# ─────────────────────────────────────────────────────────────────────────────

class SurrogateKeyGenerator:
    """Generate deterministic surrogate keys via SHA-256."""

    def __init__(self, prefix: str = "", key_length: int = 32) -> None:
        self.prefix     = prefix
        self.key_length = key_length

    def hash_key(self, natural_key: str) -> str:
        digest = hashlib.sha256(natural_key.encode("utf-8")).hexdigest()
        return f"{self.prefix}{digest[:self.key_length]}"


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication engine  (subsystem 7)
# ─────────────────────────────────────────────────────────────────────────────

class DeduplicationStrategy(str, Enum):
    KEEP_FIRST = "KEEP_FIRST"
    KEEP_LAST  = "KEEP_LAST"


class DeduplicationEngine:
    """Pandas-based deduplication engine."""

    def __init__(
        self,
        keys: List[str],
        strategy: DeduplicationStrategy = DeduplicationStrategy.KEEP_LAST,
        tiebreaker_col: Optional[str] = None,
    ) -> None:
        self.keys           = keys
        self.strategy       = strategy
        self.tiebreaker_col = tiebreaker_col

    def deduplicate_dataframe(
        self,
        df: pd.DataFrame,
        source: str = "",
        run_id: str  = "",
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        before = len(df)
        if not self.keys or not all(k in df.columns for k in self.keys):
            return df, {"before": before, "after": before, "removed": 0}

        if self.tiebreaker_col and self.tiebreaker_col in df.columns:
            df = df.sort_values(self.tiebreaker_col, ascending=True)

        keep = "last" if self.strategy == DeduplicationStrategy.KEEP_LAST else "first"
        df   = df.drop_duplicates(subset=self.keys, keep=keep)
        after = len(df)

        return df.reset_index(drop=True), {
            "before": before, "after": after, "removed": before - after,
            "source": source, "run_id": run_id,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Data profiler  (subsystem 1)
# ─────────────────────────────────────────────────────────────────────────────

class DataProfiler:
    """Basic data profiling: null counts, distinct counts, row count."""

    @staticmethod
    def profile(df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "row_count":      len(df),
            "column_count":   len(df.columns),
            "null_counts":    df.isnull().sum().to_dict(),
            "distinct_counts":{c: df[c].nunique() for c in df.columns},
        }


# ─────────────────────────────────────────────────────────────────────────────
# Cleansing rule set  (subsystem 4)
# ─────────────────────────────────────────────────────────────────────────────

class CleansingRuleSet:
    """
    Per-record DQ gate. check_fn signature: (record: dict) → (passed: bool, message: str).
    """

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self._rules: List[Any] = []

    def add_rule(self, check_fn: Any) -> "CleansingRuleSet":
        self._rules.append(check_fn)
        return self

    def apply(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        messages: List[str] = []
        all_passed = True
        for fn in self._rules:
            try:
                result = fn(record)
                if isinstance(result, tuple):
                    passed, msg = result[0], result[1] if len(result) > 1 else ""
                else:
                    passed, msg = bool(result), ""
                if not passed:
                    all_passed = False
                    if msg:
                        messages.append(str(msg))
            except Exception as exc:
                all_passed = False
                messages.append(f"Rule error: {exc}")
        return all_passed, messages
