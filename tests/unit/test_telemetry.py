import json
import logging

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from shared.logger.logger_setup import JsonFormatter
from shared.telemetry import bind_run, init_telemetry, span


@pytest.fixture(scope="module")
def exporter():
    exp = InMemorySpanExporter()
    init_telemetry(span_exporter=exp)
    return exp


def test_nested_spans_share_trace_id(exporter):
    exporter.clear()
    with span("pipeline.run"):
        with span("validate_contract", contract="x"):
            pass
    child, root = exporter.get_finished_spans()
    assert child.context.trace_id == root.context.trace_id
    assert child.parent.span_id == root.context.span_id


def test_exception_marks_span_error(exporter):
    exporter.clear()
    with pytest.raises(ValueError):
        with span("bronze.step"):
            raise ValueError("boom")
    assert not exporter.get_finished_spans()[0].status.is_ok


def test_json_log_has_trace_and_run_context(exporter):
    bind_run("run_1", "cophieu68")
    rec = logging.LogRecord("t", logging.INFO, __file__, 1, "hello", None, None)
    with span("pipeline.run"):
        out = json.loads(JsonFormatter().format(rec))
    assert out["run_id"] == "run_1" and out["pipeline_name"] == "cophieu68"
    assert len(out["trace_id"]) == 32 and len(out["span_id"]) == 16


def test_json_log_without_span_has_no_trace_id(exporter):
    rec = logging.LogRecord("t", logging.INFO, __file__, 1, "hello", None, None)
    assert "trace_id" not in json.loads(JsonFormatter().format(rec))