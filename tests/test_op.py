"""Verifies DagsterInstrumentor against a real Dagster op execution -- not just
that _wrap_decorator_factory is internally consistent, but that a real
dagster.op(...) call, patched, actually produces spans when the resulting
OpDefinition runs for real via execute_in_process(). Bare and parameterized
@op forms are exercised separately since that's exactly the dispatch
_wrap_decorator_factory has to get right."""

import dagster
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry.instrumentation.dagster import DagsterInstrumentor


@pytest.fixture
def instrumented():
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    yield instrumentor
    instrumentor.uninstrument()


def test_bare_op_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.op
    def my_op(context) -> int:
        return 1

    @dagster.job
    def my_job() -> None:
        my_op()

    result = my_job.execute_in_process()
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "my_op" in span_names


def test_parameterized_op_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.op(name="renamed_op")
    def original_name(context) -> int:
        return 1

    @dagster.job
    def my_job() -> None:
        original_name()

    result = my_job.execute_in_process()
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "renamed_op" in span_names


def test_uninstrument_stops_patching(spans: InMemorySpanExporter) -> None:
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    instrumentor.uninstrument()

    @dagster.op
    def unpatched_op(context) -> int:
        return 1

    @dagster.job
    def my_job() -> None:
        unpatched_op()

    result = my_job.execute_in_process()
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "unpatched_op" not in span_names
