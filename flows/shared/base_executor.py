"""
flows/shared/base_executor.py
==============================
BaseExecutor — interface chung cho tất cả phase executors.

Mỗi pipeline có 4 executors: BronzeExecutor, SilverExecutor, GoldExecutor, ServingExecutor.
Chúng đều implement interface này để Orchestrator có thể gọi uniform qua execute().

Nguyên tắc:
  - ISP: interface tối giản — chỉ execute() + optional pre/post hooks.
  - OCP: thêm pipeline mới không sửa Orchestrator, chỉ tạo Executor mới.
  - LSP: tất cả Executor đều thay thế được nhau từ góc nhìn của Orchestrator.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseExecutor(ABC):
    """
    Abstract base cho tất cả phase executors (Bronze / Silver / Gold / Serving).

    Constructor convention (subclass nên giữ):
        def __init__(self, context: ExecutionContext, config: BasePipelineConfig) -> None:
            self.context = context
            self.config  = config
            self.logger  = context.logger
    """

    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """
        Chạy toàn bộ phase, trả về result dict.

        Result dict phải có ít nhất:
          - "phase"  : str  — tên phase ("bronze" / "silver" / ...)
          - "errors" : int  — số lỗi gặp phải (0 = không lỗi)
        """

    # ------------------------------------------------------------------
    # Optional lifecycle hooks — subclass override nếu cần
    # ------------------------------------------------------------------

    def pre_execute(self) -> None:
        """Chạy trước execute(). Dùng để setup, acquire resources."""

    def post_execute(self, result: Dict[str, Any]) -> None:
        """Chạy sau execute(). Dùng để cleanup, emit metrics."""

    # ------------------------------------------------------------------
    # Helper — log lineage nếu context có metadata_repo
    # ------------------------------------------------------------------

    def _log_lineage(
        self,
        run_id: str,
        source_layer: str,
        source_table: str,
        target_layer: str,
        target_table: str,
        operation: str,
        rows: int,
    ) -> None:
        if hasattr(self, "context") and self.context.metadata_repo:
            try:
                self.context.metadata_repo.log_lineage(
                    run_id=run_id,
                    source_layer=source_layer,
                    source_table=source_table,
                    target_layer=target_layer,
                    target_table=target_table,
                    operation=operation,
                    rows_affected=rows,
                )
            except Exception:
                pass  # lineage là optional, không được làm crash pipeline
