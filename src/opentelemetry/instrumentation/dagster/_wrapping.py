"""The wrapt dispatch logic that actually applies dagster_otel.traced() (or,
for a @dbt_assets-originated multi_asset call, dagster_otel.dbt.traced_dbt())
to a Dagster decorator factory's incoming compute function. Split out from
__init__.py (which just wires this up to the specific dagster.op/asset/
multi_asset names and the BaseInstrumentor lifecycle) so it can be
unit-tested in isolation."""

import inspect
from collections.abc import Callable
from typing import Any

from dagster_otel import traced
from dagster_otel.dbt import traced_dbt

#: The exact (module, function) pair `dagster_dbt.asset_decorator.dbt_assets`
#: appears as in a stack frame when it calls `multi_asset(specs=..., ...)`
#: internally (confirmed live, both against a real dbt manifest and a
#: hand-rolled one -- see docs/design.md's "Dispatching @dbt_assets to
#: traced_dbt()" section). Checked as a pair, not "any dagster_dbt frame":
#: `dagster_dbt.cloud.ops` also calls `multi_asset` internally, for a
#: completely unrelated feature (Cloud job assets, no dbt manifest/specs
#: involved at all) -- a module-only check would misfire on that and route it
#: through `traced_dbt()`, which expects the dbt-node/asset_key correlation
#: `dbt_assets`-produced compute functions actually provide.
_DBT_ASSETS_FRAME = ("dagster_dbt.asset_decorator", "dbt_assets")


def _called_from_dbt_assets() -> bool:
    """Whether the `multi_asset(...)` call currently being intercepted
    originated from `dagster_dbt.asset_decorator.dbt_assets`, not a user's own
    hand-written `@multi_asset(specs=...)`.

    Stack inspection, not kwargs (`specs`/`check_specs` alone aren't a
    reliable enough signal -- a user's own `@multi_asset` can pass either) --
    an accepted-risk dependence on `dagster_dbt`'s internal call shape, same
    category as this project's other private-API reliance (see README.md's
    Compatibility section). Only called when `wrapped.__name__ ==
    "multi_asset"` (see `_wrap_decorator_factory`), so `@op`/`@asset`/
    `@asset_check` decoration never pays for this stack walk at all.
    """
    return any(
        (frame.frame.f_globals.get("__name__"), frame.function) == _DBT_ASSETS_FRAME
        for frame in inspect.stack()
    )


def _wrap_decorator_factory(
    wrapped: Callable[..., Any], instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Any:
    """Wraps a Dagster decorator factory (dagster.op/dagster.asset/dagster.
    multi_asset/dagster.asset_check) that supports `@op`/`@asset` (bare --
    first positional arg is the compute function itself) and
    `@op(name=...)`/`@asset(name=...)`/`@multi_asset(outs=...)`/
    `@asset_check(asset=...)` (parameterized -- returns a decorator, applied
    later) forms. `multi_asset` and `asset_check` are both keyword-only (no
    bare form at all, per their own signatures: `def multi_asset(*,
    outs=None, ...)`, `def asset_check(*, asset, ...)`), so they only ever
    exercise the parameterized branch below -- no extra dispatch logic
    needed. See docs/design.md's "Sketch of the decorator wrapper" for the
    reasoning.

    Passes a `name=...` override through to `traced(span_name=...)` when given
    -- confirmed against a real @op(name="renamed_op") run that traced()'s own
    default (falling back to the compute function's __name__) otherwise
    produces a span named after the Python function, not the name Dagster
    actually gives the op/step. Manual @traced() usage can't fix this itself
    (decorator order means it never sees @op's own kwargs); this package can,
    since it's the thing patching dagster.op(name=...) directly.

    A `multi_asset` call that's really a `@dbt_assets`-produced one (Issue
    #14) gets `traced_dbt()` instead of plain `traced()` -- one child span per
    dbt node, matching what a manual `@traced_dbt()` would give, instead of
    one span for the whole dbt run."""
    if args and callable(args[0]) and not kwargs:
        fn, *rest = args
        return wrapped(traced()(fn), *rest, **kwargs)

    real_decorator = wrapped(*args, **kwargs)
    span_name = kwargs.get("name")
    trace_factory = (
        traced_dbt if wrapped.__name__ == "multi_asset" and _called_from_dbt_assets() else traced
    )

    def patched_decorator(fn: Callable[..., Any]) -> Any:
        return real_decorator(trace_factory(span_name)(fn))

    return patched_decorator
