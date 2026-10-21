"""
flows/common/ — Shared pipeline infrastructure.

Public API:
    from flows.common import (
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
from flows.common.base_config import (
    BasePipelineConfig,
    BaseExecutor,
    BasePipelineOrchestrator,
    format_pipeline_result,
)

__all__ = [
    "ExecutionContext",
    "ExecutionPhase",
    "ProcessingBackend",
    "make_run_id",
    "make_batch_id",
    "BasePipelineConfig",
    "BaseExecutor",
    "BasePipelineOrchestrator",
    "format_pipeline_result",
]
