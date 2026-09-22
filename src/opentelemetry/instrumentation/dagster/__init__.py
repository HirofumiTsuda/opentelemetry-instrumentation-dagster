"""Auto-instrumentation for Dagster ops/assets/asset checks.

Patches dagster.op/dagster.asset/dagster.multi_asset/dagster.asset_check
(public, stable decorator factories) so each applies dagster_otel.traced() to
the incoming compute function before Dagster ever builds the resulting
OpDefinition/AssetsDefinition/AssetChecksDefinition -- the actual wrapt
dispatch logic lives in _wrapping.py, this module just wires it up to those
four names and the BaseInstrumentor lifecycle. dagster_dbt.dbt_assets is
covered for free by the multi_asset patch -- it just calls
dagster.multi_asset(specs=..., ...) internally and returns the result, no
independent code path (see README.md's "multi_asset covers @dbt_assets for
free" section). dagster.graph_asset is deliberately NOT covered -- see
README.md's "graph_asset is out of scope" section for why applying traced()
there would be actively wrong, not just unnecessary.
"""

import sys
import warnings
from typing import Any

import wrapt

# opentelemetry-instrumentation's own instrumentor.py has a file-level `# type:
# ignore`, so mypy sees no type info for it at all -- not a gap in this file.
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap

import dagster

from ._wrapping import _wrap_decorator_factory
from .version import __version__

__all__ = ["DagsterInstrumentor", "__version__"]


class DagsterInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return ("dagster >= 1.5",)

    def _instrument(self, **kwargs: Any) -> None:
        # dagster_dbt.asset_decorator does `from dagster import ... multi_asset ...`
        # at its own module import time -- a one-time name binding, not a live link
        # back to dagster's attribute (see README.md's "multi_asset covers
        # @dbt_assets for free" section, and tests/test_dbt_assets.py, which
        # reproduces both directions directly). If dagster_dbt is already imported
        # by the time this runs, its own multi_asset reference is permanently
        # frozen to the pre-patch function -- @dbt_assets silently stops being
        # traced, with nothing else to indicate anything is wrong. Warn instead of
        # staying silent about it.
        if "dagster_dbt" in sys.modules:
            warnings.warn(
                "dagster_dbt was already imported before "
                "opentelemetry-instrumentation-dagster's instrument() ran -- "
                "@dbt_assets will not be traced, since dagster_dbt's own reference "
                "to dagster.multi_asset is already bound to the pre-patch function. "
                "Run your program under `opentelemetry-instrument` (which patches "
                "before any of your own code, including dagster_dbt, gets "
                "imported), or call instrument() before importing dagster_dbt.",
                stacklevel=2,
            )

        wrapt.wrap_function_wrapper("dagster", "op", _wrap_decorator_factory)
        wrapt.wrap_function_wrapper("dagster", "asset", _wrap_decorator_factory)
        wrapt.wrap_function_wrapper("dagster", "multi_asset", _wrap_decorator_factory)
        wrapt.wrap_function_wrapper("dagster", "asset_check", _wrap_decorator_factory)

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(dagster, "op")
        unwrap(dagster, "asset")
        unwrap(dagster, "multi_asset")
        unwrap(dagster, "asset_check")
