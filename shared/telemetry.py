"""
shared/telemetry.py — ST05: tracing + metrics cho pipeline (lượt 1, không cần container).

- span(name, **attrs): context manager tạo span; no-op an toàn nếu chưa init.
- bind_run(run_id, pipeline): gắn run_id vào ContextVar để JsonFormatter đưa vào mọi log.
- Metrics (prometheus_client, in-process): chỉ mở /metrics khi đặt PROMETHEUS_PORT.

Env:
    TELEMETRY_EXPORTER = none (mặc định) | console | otlp
    OTEL_EXPORTER_OTLP_ENDPOINT = http://localhost:4317 (chỉ khi exporter=otlp)
    PROMETHEUS_PORT = 8000 (tùy chọn)
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor, SpanExporter,
)
from prometheus_client import Counter, Histogram, start_http_server

SERVICE_NAME = "webcraw-pipeline"

# ── Metrics ──────────────────────────────────────────────────────────────────
PIPELINE_DURATION = Histogram(
    "pipeline_duration_seconds", "Thời gian chạy pipeline", ["pipeline", "phase"])
RECORDS_PROCESSED = Counter(
    "records_processed_total", "Số record đã xử lý", ["layer", "table"])
PIPELINE_ERRORS = Counter(
    "pipeline_errors_total", "Số lỗi pipeline", ["pipeline", "phase"])

# ── Run context (đọc bởi JsonFormatter) ──────────────────────────────────────
_run_ctx: ContextVar[Dict[str, str]] = ContextVar("run_ctx", default={})

_initialized = False


def bind_run(run_id: str, pipeline: str) -> None:
    """Gắn run_id/pipeline_name vào context hiện tại để mọi log tự mang theo."""
    _run_ctx.set({"run_id": run_id, "pipeline_name": pipeline})


def current_run_context() -> Dict[str, str]:
    return _run_ctx.get()


def init_telemetry(span_exporter: Optional[SpanExporter] = None) -> None:
    """Gọi 1 lần ở entrypoint, sau setup_pipeline_logging(). Gọi lại là no-op."""
    global _initialized
    if _initialized:
        return
    provider = TracerProvider(resource=Resource.create({"service.name": SERVICE_NAME}))

    kind = os.getenv("TELEMETRY_EXPORTER", "none").lower()
    if span_exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    elif kind == "console":
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    elif kind == "otlp":
        # Import trong nhánh: exporter OTLP là dependency tùy chọn.
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    # kind == "none": không exporter, span vẫn có trace_id/span_id cho log.

    trace.set_tracer_provider(provider)

    port = os.getenv("PROMETHEUS_PORT")
    if port:
        start_http_server(int(port))
    _initialized = True


@contextmanager
def span(name: str, **attrs: Any) -> Iterator[trace.Span]:
    """Tạo child span. Exception tự được ghi vào span rồi raise tiếp."""
    tracer = trace.get_tracer(SERVICE_NAME)
    with tracer.start_as_current_span(name) as sp:
        for k, v in attrs.items():
            if v is not None:
                sp.set_attribute(k, v)
        yield sp


def current_trace_ids() -> Dict[str, str]:
    """trace_id/span_id dạng hex của span hiện tại; rỗng nếu không có span hợp lệ."""
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return {}
    return {"trace_id": format(ctx.trace_id, "032x"), "span_id": format(ctx.span_id, "016x")}