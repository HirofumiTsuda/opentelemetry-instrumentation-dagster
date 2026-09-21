"""Verifies dagster_dbt.dbt_assets rides along on the multi_asset patch for
free -- confirmed by reading dagster_dbt/asset_decorator.py directly (see
README.md's "multi_asset covers @dbt_assets for free" section): dbt_assets
just calls dagster.multi_asset(specs=..., ...) and returns the result, no
independent code path.

Not exercised here against a real dbt project/manifest (dagster-otel's own
test suite + examples/ already cover real dbt materialization end to end;
duplicating that heavy setup here wouldn't test anything new about THIS
package). What's specific to this package, and what these tests actually
verify, is the import-order mechanism the "for free" claim depends on:
dagster_dbt.asset_decorator does `from dagster import ... multi_asset ...`
at its own module import time -- a one-time name binding, not a live link
back to the dagster module's attribute. So whether dagster_dbt ends up
sharing the *patched* multi_asset or the original one depends entirely on
whether it gets imported before or after instrument() runs."""

import sys

import dagster
import pytest

from opentelemetry.instrumentation.dagster import DagsterInstrumentor


@pytest.fixture(autouse=True)
def _fresh_dagster_dbt_import():
    """dagster_dbt.asset_decorator's `from dagster import multi_asset` binding
    only happens the first time it's imported in this process -- drop it from
    sys.modules before and after each test so every test here gets a genuine
    fresh import, not whatever a previous test happened to leave cached."""
    sys.modules.pop("dagster_dbt", None)
    sys.modules.pop("dagster_dbt.asset_decorator", None)
    yield
    sys.modules.pop("dagster_dbt", None)
    sys.modules.pop("dagster_dbt.asset_decorator", None)


def test_multi_asset_patched_before_dagster_dbt_import_is_shared() -> None:
    """The supported order: opentelemetry-instrument's sitecustomize.py runs
    instrument() before any user code -- including dagster_dbt -- ever gets
    imported."""
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    try:
        import dagster_dbt.asset_decorator

        # Deliberately reaching into dagster_dbt's own internal name binding --
        # not a public API, that's exactly the mechanism under test here.
        assert (
            dagster_dbt.asset_decorator.multi_asset  # pyright: ignore[reportPrivateImportUsage]
            is dagster.multi_asset
        )
    finally:
        instrumentor.uninstrument()


def test_multi_asset_patched_after_dagster_dbt_import_is_not_shared() -> None:
    """The documented gotcha, reproduced directly rather than just asserted
    in prose: if dagster_dbt is already imported before instrument() runs,
    its own `multi_asset` name is permanently bound to the pre-patch
    function -- `from X import Y` copies a reference, it doesn't create a
    live link back to X's attribute."""
    import dagster_dbt.asset_decorator

    # Same deliberate reach into a non-public name as the test above.
    pre_patch_reference = (
        dagster_dbt.asset_decorator.multi_asset  # pyright: ignore[reportPrivateImportUsage]
    )

    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    try:
        assert (
            dagster_dbt.asset_decorator.multi_asset  # pyright: ignore[reportPrivateImportUsage]
            is pre_patch_reference
        )
        assert (
            dagster_dbt.asset_decorator.multi_asset  # pyright: ignore[reportPrivateImportUsage]
            is not dagster.multi_asset
        )
    finally:
        instrumentor.uninstrument()


def test_no_warning_when_dagster_dbt_not_yet_imported(
    recwarn: pytest.WarningsRecorder,
) -> None:
    """The supported order shouldn't warn about anything."""
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    try:
        assert not [w for w in recwarn.list if "dagster_dbt" in str(w.message)]
    finally:
        instrumentor.uninstrument()


def test_warns_when_dagster_dbt_already_imported() -> None:
    """The gotcha the two tests above exercise the mechanics of -- surfaced as
    an actual warning instead of a silent, hard-to-diagnose tracing gap."""
    import dagster_dbt  # noqa: F401  -- import itself is the point, not its use

    instrumentor = DagsterInstrumentor()
    try:
        with pytest.warns(UserWarning, match="dagster_dbt"):
            instrumentor.instrument()
    finally:
        instrumentor.uninstrument()
