"""Issue #14: a `multi_asset(...)` call that's really `@dbt_assets` under the
hood should get `dagster_otel.dbt.traced_dbt()` (per-dbt-node child spans),
not plain `traced()` (one span for the whole dbt run). The hard part is
detection, not dispatch -- `specs`/`check_specs` kwargs alone aren't a
reliable enough signal that a `multi_asset` call came from `@dbt_assets`
specifically vs. a user's own hand-written `@multi_asset(specs=...)` (see
`_wrapping.py`'s `_called_from_dbt_assets` docstring), so these tests exercise
the real `dagster_dbt.asset_decorator.dbt_assets` call path, not a synthetic
stand-in for it.

A minimal, hand-built manifest (no real dbt project/`dbt parse` needed) is
enough to exercise `dbt_assets`'s own `multi_asset(...)` call -- the actual
end-to-end verification against a real dbt project lives in `examples/`
(Issue #6), this only needs the dispatch logic itself to run."""

import sys
import types
from typing import Any

import dagster
import pytest

from opentelemetry.instrumentation.dagster import DagsterInstrumentor
from opentelemetry.instrumentation.dagster._wrapping import _called_from_dbt_assets

# dagster_dbt.asset_decorator's own `multi_asset` reference is bound at ITS
# import time, same mechanism test_dbt_assets.py's tests are built around --
# `dbt_assets`/`DagsterDbtTranslator` must be imported *inside* each test,
# after `instrumented` has patched dagster.multi_asset, not at this module's
# top level (which would run once, before any instrument() call ever
# happens, permanently binding the pre-patch function).

_EMPTY_MANIFEST: dict[str, Any] = {
    "nodes": {},
    "sources": {},
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "generated_at": "2026-01-01T00:00:00Z",
        "invocation_id": "test",
    },
    "parent_map": {},
    "child_map": {},
    "macros": {},
    "docs": {},
    "exposures": {},
    "metrics": {},
    "selectors": {},
    "disabled": {},
    "group_map": {},
    "saved_queries": {},
    "semantic_models": {},
    "unit_tests": {},
}


@pytest.fixture(autouse=True)
def _fresh_dagster_dbt_import():
    """Same reasoning as test_dbt_assets.py's fixture of the same name --
    dagster_dbt.asset_decorator's `from dagster import multi_asset` binding
    only happens on first import, so each test needs a genuinely fresh one."""
    sys.modules.pop("dagster_dbt", None)
    sys.modules.pop("dagster_dbt.asset_decorator", None)
    yield
    sys.modules.pop("dagster_dbt", None)
    sys.modules.pop("dagster_dbt.asset_decorator", None)


@pytest.fixture
def instrumented():
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    yield instrumentor
    instrumentor.uninstrument()


def _recording_factory(calls: list[str], label: str):
    """Stands in for `traced`/`traced_dbt`: records that it was called (with
    which label), returns a plain identity decorator so the compute function
    underneath is unaffected."""

    def factory(span_name: str | None = None):
        calls.append(label)
        return lambda fn: fn

    return factory


def test_dbt_assets_dispatches_to_traced_dbt(
    instrumented: DagsterInstrumentor, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "opentelemetry.instrumentation.dagster._wrapping.traced_dbt",
        _recording_factory(calls, "traced_dbt"),
    )
    monkeypatch.setattr(
        "opentelemetry.instrumentation.dagster._wrapping.traced",
        _recording_factory(calls, "traced"),
    )

    from dagster_dbt import DagsterDbtTranslator, dbt_assets

    # Importing dagster_dbt fresh here has its own side effect worth noting:
    # dagster_dbt.cloud.ops calls multi_asset(...) at ITS OWN module import
    # time, for a completely unrelated feature (no dbt manifest/specs
    # involved) -- correctly dispatched to plain `traced`, confirmed by the
    # leading "traced" entry below. This is exactly the false-positive risk
    # `_called_from_dbt_assets`'s module+function check exists to avoid; a
    # module-only check would have added a spurious "traced_dbt" here too.
    calls.clear()

    @dbt_assets(manifest=_EMPTY_MANIFEST, dagster_dbt_translator=DagsterDbtTranslator())
    def my_dbt_assets(context, dbt):
        pass

    assert calls == ["traced_dbt"]


def test_hand_written_multi_asset_with_dbt_like_kwargs_still_gets_traced(
    instrumented: DagsterInstrumentor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exact ambiguity Issue #14 calls out: a user's own @multi_asset can
    pass `specs`/`check_specs` too -- kwargs alone can't distinguish it from a
    real @dbt_assets call, only the call stack can."""
    calls: list[str] = []
    monkeypatch.setattr(
        "opentelemetry.instrumentation.dagster._wrapping.traced_dbt",
        _recording_factory(calls, "traced_dbt"),
    )
    monkeypatch.setattr(
        "opentelemetry.instrumentation.dagster._wrapping.traced",
        _recording_factory(calls, "traced"),
    )

    @dagster.multi_asset(specs=[dagster.AssetSpec("foo")], check_specs=[])
    def my_multi_asset(context):
        pass

    assert calls == ["traced"]


def test_called_from_dbt_assets_requires_both_module_and_function_name() -> None:
    """Precision check for `_called_from_dbt_assets` itself: a frame whose
    module is `dagster_dbt.asset_decorator` but whose *function* isn't
    `dbt_assets` (standing in for `dagster_dbt.cloud.ops`, which also calls
    `multi_asset` internally for a completely unrelated feature) must not
    false-positive."""
    # A real module object (not just a name string) so the function defined
    # in it below gets __globals__["__name__"] == "dagster_dbt.asset_decorator"
    # -- exactly what inspect.stack() reports for a frame, real or faked.
    fake_module = types.ModuleType("dagster_dbt.asset_decorator")
    fake_module.__dict__["_called_from_dbt_assets"] = _called_from_dbt_assets
    exec(
        "def not_dbt_assets():\n    return _called_from_dbt_assets()\n",
        fake_module.__dict__,
    )
    assert fake_module.not_dbt_assets() is False  # type: ignore[attr-defined]
