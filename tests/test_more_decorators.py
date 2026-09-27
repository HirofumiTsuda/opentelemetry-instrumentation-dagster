"""Issue #36: `@multi_asset_check`, `@observable_source_asset` and
`@multi_observable_source_asset` are patched too, verified against real Dagster
execution. Each already worked with a manual @traced(); auto-instrumentation just
never applied it, so these steps silently got no span."""

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


def _observe(assets: list, keys: list[str]) -> bool:  # type: ignore[type-arg]
    defs = dagster.Definitions(assets=assets)
    job = dagster.define_asset_job("observe", selection=keys).resolve(
        asset_graph=defs.resolve_asset_graph()
    )
    return job.execute_in_process().success


def test_multi_asset_check_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.asset
    def checked() -> int:
        return 1

    @dagster.multi_asset_check(
        specs=[
            dagster.AssetCheckSpec("c1", asset=checked),
            dagster.AssetCheckSpec("c2", asset=checked),
        ]
    )
    def checks():  # type: ignore[no-untyped-def]
        yield dagster.AssetCheckResult(check_name="c1", asset_key=checked.key, passed=True)
        yield dagster.AssetCheckResult(check_name="c2", asset_key=checked.key, passed=True)

    assert dagster.materialize([checked, checks]).success

    by_name = {s.name: s for s in spans.get_finished_spans()}
    assert sorted(by_name) == ["checked", "checks"]
    attributes = by_name["checks"].attributes
    assert attributes is not None
    assert attributes["dagster.asset_check_keys"] == "checked:c1,checked:c2"


@pytest.mark.parametrize("form", ["bare", "parameterized"])
def test_observable_source_asset_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter, form: str
) -> None:
    if form == "bare":

        @dagster.observable_source_asset
        def src() -> dagster.DataVersion:
            return dagster.DataVersion("v1")

    else:

        @dagster.observable_source_asset(key=["raw", "src"])
        def src() -> dagster.DataVersion:
            return dagster.DataVersion("v1")

    key = "src" if form == "bare" else "raw/src"
    assert _observe([src], [key])

    spans_by_name = {s.name: s for s in spans.get_finished_spans()}
    expected_name = "src" if form == "bare" else "raw__src"
    assert list(spans_by_name) == [expected_name]
    attributes = spans_by_name[expected_name].attributes
    assert attributes is not None
    assert attributes["dagster.asset_keys"] == key


def test_multi_observable_source_asset_gets_a_span(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.multi_observable_source_asset(specs=[dagster.AssetSpec("s1"), dagster.AssetSpec("s2")])
    def srcs():  # type: ignore[no-untyped-def]
        yield dagster.ObserveResult(asset_key="s1", data_version=dagster.DataVersion("a"))
        yield dagster.ObserveResult(asset_key="s2", data_version=dagster.DataVersion("b"))

    assert _observe([srcs], ["s1", "s2"])

    finished = spans.get_finished_spans()
    assert [s.name for s in finished] == ["srcs"]
    attributes = finished[0].attributes
    assert attributes is not None
    assert attributes["dagster.asset_keys"] == "s1,s2"


def test_uninstrument_restores_the_new_decorators(spans: InMemorySpanExporter) -> None:
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    instrumentor.uninstrument()

    @dagster.observable_source_asset
    def untraced_src() -> dagster.DataVersion:
        return dagster.DataVersion("v1")

    assert _observe([untraced_src], ["untraced_src"])
    assert spans.get_finished_spans() == ()
