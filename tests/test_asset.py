"""Verifies DagsterInstrumentor's @asset support against real Dagster
materialization -- same dispatch shape as @op (see test_op.py), reusing
_wrap_decorator_factory unchanged, so these tests focus on what's specific to
assets: a real Definitions/materialize_to_memory() run, and confirming
@graph_asset is genuinely left untouched (not just "not covered" but actually
still works, since applying traced() to its compose function would be wrong
per README.md, not merely unimplemented)."""

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


def test_bare_asset_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.asset
    def my_asset(context) -> int:
        return 1

    result = dagster.materialize([my_asset])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "my_asset" in span_names


def test_parameterized_asset_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.asset(name="renamed_asset")
    def original_name(context) -> int:
        return 1

    result = dagster.materialize([original_name])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "renamed_asset" in span_names


def test_graph_asset_is_left_untouched(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    """The graph_asset's own compose function (no context arg) must not be
    wrapped -- if it were, calling traced()'s span-creating wrapper around it
    would break at the first context.log call inside _traced_span, since
    compose functions never receive a context at all. Tracing the ops it
    composes still works today, for free, since those are plain @op's."""

    @dagster.op
    def fetch(context) -> int:
        return 1

    @dagster.op
    def store(context, x: int) -> None:
        pass

    @dagster.graph_asset
    def my_graph_asset():
        return store(fetch())

    result = dagster.materialize([my_graph_asset])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "fetch" in span_names
    assert "store" in span_names
