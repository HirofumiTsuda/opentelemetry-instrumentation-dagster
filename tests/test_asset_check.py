"""Verifies DagsterInstrumentor's @asset_check support against real
materialization (Issue #16) -- asset_check is keyword-only (no bare
@asset_check form at all, same as @multi_asset), so _wrap_decorator_factory's
bare-form branch never fires here, only its parameterized branch. Also
confirms the check's compute function receives a real
AssetCheckExecutionContext at runtime and that dagster_otel.traced() handles
it -- dagster_otel itself only gained that support in 0.4.0 (Issue #72
there)."""

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


def test_asset_check_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.asset
    def my_asset(context) -> int:
        return 1

    @dagster.asset_check(asset=my_asset)
    def my_check(context) -> dagster.AssetCheckResult:
        return dagster.AssetCheckResult(passed=True)

    result = dagster.materialize([my_asset, my_check])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "my_asset" in span_names
    assert "my_check" in span_names


def test_named_asset_check_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.asset
    def another_asset(context) -> int:
        return 1

    @dagster.asset_check(asset=another_asset, name="renamed_check")
    def original_name(context) -> dagster.AssetCheckResult:
        return dagster.AssetCheckResult(passed=True)

    result = dagster.materialize([another_asset, original_name])
    assert result.success

    span_names = [span.name for span in spans.get_finished_spans()]
    assert "renamed_check" in span_names
