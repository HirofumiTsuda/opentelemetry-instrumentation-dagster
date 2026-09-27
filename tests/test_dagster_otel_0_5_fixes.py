"""Regression tests for Issues #32-#35, all fixed by moving the dagster-otel floor to
0.5.0: each was a traced() behavior this package inherited by applying traced() to
every compute function automatically (dagster-otel#94, #93, #96, #95 respectively).

Run through real Dagster with DagsterInstrumentor active, since each failure mode
lives in how Dagster itself drives the patched decorator: signature inspection
(#32), async detection and per-item Tasks (#33), run-tag parentage across steps
(#34), and Dagster's own node naming (#35)."""

import asyncio

import dagster
import pytest
from dagster_otel import traced
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry.instrumentation.dagster import DagsterInstrumentor


@pytest.fixture
def instrumented():
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    yield instrumentor
    instrumentor.uninstrument()


def _spans_by_name(spans: InMemorySpanExporter) -> dict:
    finished = spans.get_finished_spans()
    by_name = {s.name: s for s in finished}
    assert len(by_name) == len(finished), [s.name for s in finished]  # one span per name
    return by_name


def test_context_less_compute_functions_run_and_are_traced(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    """Issue #32: `@asset def x(): ...` used to fail with `missing 1 required
    positional argument: 'context'` as soon as the package was installed."""

    @dagster.asset
    def no_ctx_asset() -> int:
        return 1

    @dagster.asset_check(asset=no_ctx_asset)
    def no_ctx_check() -> dagster.AssetCheckResult:
        return dagster.AssetCheckResult(passed=True)

    @dagster.op
    def no_ctx_op() -> None:
        pass

    assert dagster.materialize([no_ctx_asset, no_ctx_check]).success
    job = dagster.GraphDefinition(name="no_ctx_job", node_defs=[no_ctx_op]).to_job()
    assert job.execute_in_process().success

    assert sorted(_spans_by_name(spans)) == [
        "no_ctx_asset",
        "no_ctx_asset_no_ctx_check",
        "no_ctx_op",
    ]


def test_async_compute_functions_run_and_span_covers_the_work(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    """Issue #33: async def used to fail with `cannot pickle 'coroutine' object`, and
    the span only covered creating the coroutine."""

    @dagster.asset
    async def coro_asset() -> int:
        await asyncio.sleep(0.1)
        return 1

    @dagster.multi_asset(outs={"first": dagster.AssetOut(), "second": dagster.AssetOut()})
    async def agen_assets():  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.1)
        yield dagster.Output(1, output_name="first")
        yield dagster.Output(2, output_name="second")

    result = dagster.materialize([coro_asset, agen_assets])

    assert result.success
    assert result.output_for_node("coro_asset") == 1
    by_name = _spans_by_name(spans)
    for name in ("coro_asset", "agen_assets"):
        span = by_name[name]
        assert (span.end_time - span.start_time) / 1e9 >= 0.1


def test_already_traced_function_gets_one_span_and_downstream_parents_onto_it(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    """Issue #34: an explicit @traced() under the patched @asset used to be wrapped
    again -- two spans per step, and the downstream step parented onto the inner
    one. The user's own decorator, and its span name, win."""

    @dagster.asset
    @traced("custom_name")
    def upstream() -> int:
        return 1

    @dagster.asset
    def downstream(upstream: int) -> int:
        return upstream + 1

    assert dagster.materialize([upstream, downstream]).success

    by_name = _spans_by_name(spans)
    assert sorted(by_name) == ["custom_name", "downstream"]
    parent = by_name["downstream"].parent
    assert parent is not None and by_name["custom_name"].context is not None
    assert parent.span_id == by_name["custom_name"].context.span_id


def test_span_names_follow_dagster_node_names(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    """Issue #35: only `name=` used to be honored; `key=`-based factories all shared
    the inner Python function's name."""

    def make(table: str) -> dagster.AssetsDefinition:
        @dagster.asset(key=["warehouse", table])
        def _asset() -> int:
            return 1

        return _asset

    @dagster.asset(key_prefix=["raw"])
    def orders() -> int:
        return 1

    assert dagster.materialize([make("customers"), make("payments"), orders]).success

    assert sorted(_spans_by_name(spans)) == [
        "raw__orders",
        "warehouse__customers",
        "warehouse__payments",
    ]
