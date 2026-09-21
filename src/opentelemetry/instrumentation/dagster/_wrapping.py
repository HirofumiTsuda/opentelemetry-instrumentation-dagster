"""The wrapt dispatch logic that actually applies dagster_otel.traced() to a
Dagster decorator factory's incoming compute function. Split out from
__init__.py (which just wires this up to the specific dagster.op/asset/
multi_asset names and the BaseInstrumentor lifecycle) so it can be
unit-tested in isolation, and to keep room for the dbt-detection dispatch
logic (routing @dbt_assets-originated multi_asset calls to traced_dbt()
instead of plain traced()) without that logic and the BaseInstrumentor
subclass sharing one file."""

from collections.abc import Callable
from typing import Any

from dagster_otel import traced


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
    needed. See docs/design.md's "Sketch of the decorator wrapper" for the
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
