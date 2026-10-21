"""
flows/common/context.py
========================
Shared execution primitives.

- ExecutionPhase    : BRONZE / SILVER / FULL
- ProcessingBackend : POLARS
- ExecutionContext  : runtime state truyền xuyên pipeline
- make_run_id()     : unique run ID
- make_batch_id()   : unique batch ID per symbol
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ExecutionPhase(str, Enum):
    BRONZE   = "bronze"
    SILVER   = "silver"
    FULL     = "full"


class ProcessingBackend(str, Enum):
    POLARS = "polars"

# ---------------------------------------------------------------------------
# ID generators
# ---------------------------------------------------------------------------

def make_run_id() -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"run_{ts}_{uid}"


def make_batch_id(symbol: str) -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"batch_{symbol.upper()}_{ts}_{uid}"


# ---------------------------------------------------------------------------
# ExecutionContext — runtime state, truyền xuyên suốt pipeline
# ---------------------------------------------------------------------------

@dataclass
class ExecutionContext:
    """
    Immutable runtime state của một pipeline run.

    Được tạo bởi Orchestrator.run() và truyền vào tất cả Executors.
    Executors chỉ được ghi vào: status, end_time, error_log.
    """
    run_id:        str
    phase:         ExecutionPhase
    symbols:       List[str]
    backend:       ProcessingBackend
    target_date:   str
    environment:   str             = "dev"
    dry_run:       bool            = False
    start_time:    datetime        = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    end_time:      Optional[datetime] = None
    status:        str             = "RUNNING"
    error_message: Optional[str]   = None
    metadata_repo: Optional[Any]   = None
    error_log:     Optional[Any]   = None
    logger:        Optional[logging.Logger] = None

    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()
