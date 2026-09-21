"""Verifies DagsterInstrumentor's multi_asset support against real
materialization -- multi_asset is keyword-only (no bare @multi_asset form at
all), so _wrap_decorator_factory's bare-form branch never fires here, only
its parameterized branch. Two outputs, matching the real-backend shape
dagster-otel's own Issue #37 already verified for @traced() itself."""

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


def test_multi_asset_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.multi_asset(outs={"zeta_asset": dagster.AssetOut(), "alpha_asset": dagster.AssetOut()})
    def my_multi_asset(context):
        yield dagster.Output(1, output_name="zeta_asset")
        yield dagster.Output(2, output_name="alpha_asset")

    result = dagster.materialize([my_multi_asset])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "my_multi_asset" in span_names


def test_named_multi_asset_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.multi_asset(
        name="renamed_multi_asset",
        outs={"zeta_asset": dagster.AssetOut(), "alpha_asset": dagster.AssetOut()},
    )
    def original_name(context):
        yield dagster.Output(1, output_name="zeta_asset")
        yield dagster.Output(2, output_name="alpha_asset")

    result = dagster.materialize([original_name])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "renamed_multi_asset" in span_names
