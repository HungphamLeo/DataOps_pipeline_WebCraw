"""
flows/shared/ — Shared pipeline infrastructure.

Tất cả các thành phần tái sử dụng được cho mọi pipeline đặt tại đây.
Pipeline cụ thể chỉ import, không implement lại.

Public API:
    from flows.shared import (
        BasePipelineConfig,
        BaseExecutor,
        BasePipelineOrchestrator,
        ExecutionContext,
        ExecutionPhase,
        ProcessingBackend,
        make_run_id,
        make_batch_id,
        format_pipeline_result,
    )
"""
from flows.common.context import (
    ExecutionContext,
    ExecutionPhase,
    ProcessingBackend,
    make_run_id,
    make_batch_id,
)
from flows.common.base_config import BasePipelineConfig
from flows.common.base_executor import BaseExecutor
from flows.common.base_orchestrator import BasePipelineOrchestrator, format_pipeline_result

__all__ = [
    # context primitives
    "ExecutionContext",
    "ExecutionPhase",
    "ProcessingBackend",
    "make_run_id",
    "make_batch_id",
    # shared base classes
    "BasePipelineConfig",
    "BaseExecutor",
    "BasePipelineOrchestrator",
    "format_pipeline_result",
]
