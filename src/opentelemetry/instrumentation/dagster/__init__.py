"""Auto-instrumentation for Dagster ops/assets.

Patches dagster.op/dagster.asset/dagster.multi_asset (public, stable
decorator factories) so each applies dagster_otel.traced() to the incoming
compute function before Dagster ever builds the resulting OpDefinition/
AssetsDefinition. dagster_dbt.dbt_assets is covered for free by the
multi_asset patch -- it just calls dagster.multi_asset(specs=..., ...)
internally and returns the result, no independent code path (see README.md's
"multi_asset covers @dbt_assets for free" section). dagster.graph_asset is
deliberately NOT covered -- see README.md's "graph_asset is out of scope"
section for why applying traced() there would be actively wrong, not just
unnecessary.
"""

import sys
import warnings
from collections.abc import Callable
from typing import Any

import wrapt
from dagster_otel import traced

# opentelemetry-instrumentation's own instrumentor.py has a file-level `# type:
# ignore`, so mypy sees no type info for it at all -- not a gap in this file.
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap

import dagster

from .version import __version__

__all__ = ["DagsterInstrumentor", "__version__"]


def _wrap_decorator_factory(
    wrapped: Callable[..., Any], instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Any:
    """Wraps a Dagster decorator factory (dagster.op/dagster.asset/dagster.
    multi_asset) that supports `@op`/`@asset` (bare -- first positional arg is
    the compute function itself) and `@op(name=...)`/`@asset(name=...)`/
    `@multi_asset(outs=...)` (parameterized -- returns a decorator, applied
    later) forms. `multi_asset` is keyword-only (no bare form at all, per its
    own signature: `def multi_asset(*, outs=None, ...)`), so it only ever
    exercises the parameterized branch below -- no extra dispatch logic
    needed. See README.md's "Sketch of the decorator wrapper" for the
    reasoning.

    Passes a `name=...` override through to `traced(span_name=...)` when given
    -- confirmed against a real @op(name="renamed_op") run that traced()'s own
    default (falling back to the compute function's __name__) otherwise
    produces a span named after the Python function, not the name Dagster
    actually gives the op/step. Manual @traced() usage can't fix this itself
    (decorator order means it never sees @op's own kwargs); this package can,
    since it's the thing patching dagster.op(name=...) directly."""
    if args and callable(args[0]) and not kwargs:
        fn, *rest = args
        return wrapped(traced()(fn), *rest, **kwargs)

    real_decorator = wrapped(*args, **kwargs)
    span_name = kwargs.get("name")

    def patched_decorator(fn: Callable[..., Any]) -> Any:
        return real_decorator(traced(span_name)(fn))

    return patched_decorator


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

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(dagster, "op")
        unwrap(dagster, "asset")
        unwrap(dagster, "multi_asset")
