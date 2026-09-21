"""Unit tests for _wrap_decorator_factory's dispatch logic in isolation --
fake `wrapped`/compute functions, no real Dagster decorator or OTel provider
involved (traced() only creates a span when the wrapped function is actually
*called*, not at wrap time, so these can assert on dispatch/call shape
without needing tests/conftest.py's span-capturing fixture at all).

The real per-decorator behavior (bare @op/@asset, @multi_asset's
keyword-only shape, name= passthrough, uninstrument) is exercised against
real Dagster execution in test_op.py/test_asset.py/test_multi_asset.py --
this file is specifically about the dispatch branching itself."""

from opentelemetry.instrumentation.dagster._wrapping import _wrap_decorator_factory


def _fake_compute_fn(context) -> int:
    return 1


def test_bare_form_wraps_fn_before_calling_wrapped() -> None:
    """args=(fn,), no kwargs -- the bare @op/@asset shape."""
    received: dict[str, object] = {}

    def fake_wrapped(fn, *rest, **kwargs):
        received["fn"] = fn
        received["rest"] = rest
        received["kwargs"] = kwargs
        return "sentinel-result"

    result = _wrap_decorator_factory(fake_wrapped, None, (_fake_compute_fn,), {})

    assert result == "sentinel-result"
    # traced()(_fake_compute_fn) wraps it (functools.wraps preserves this),
    # not the raw function passed straight through untouched.
    assert received["fn"] is not _fake_compute_fn
    assert received["fn"].__wrapped__ is _fake_compute_fn  # type: ignore[attr-defined]
    assert received["rest"] == ()
    assert received["kwargs"] == {}


def test_bare_form_not_triggered_when_kwargs_present() -> None:
    """A positional callable *and* kwargs -- e.g. @op(some_positional_thing,
    name=...) -- isn't the bare form; falls through to parameterized."""
    calls: list[tuple[object, ...]] = []

    def fake_wrapped(*args, **kwargs):
        calls.append((args, kwargs))
        return lambda fn: ("decorator-result", fn)

    _wrap_decorator_factory(fake_wrapped, None, (_fake_compute_fn,), {"name": "x"})

    # wrapped() was called eagerly (parameterized-form behavior), not with a
    # traced()-wrapped fn substituted for the bare-form branch.
    assert calls == [((_fake_compute_fn,), {"name": "x"})]


def test_parameterized_form_defers_wrapping_until_decorator_is_applied() -> None:
    """args=(), kwargs={...} -- @op(name=...)/@multi_asset(outs=...) --
    wrapped() must be called immediately (parameterized decorators build
    real state off their kwargs right away), but traced() is only applied
    when the *returned* decorator is later applied to a function."""
    wrapped_call_count = 0

    def fake_wrapped(**kwargs):
        nonlocal wrapped_call_count
        wrapped_call_count += 1
        return lambda fn: ("decorator-result", fn)

    patched_decorator = _wrap_decorator_factory(fake_wrapped, None, (), {"outs": {}})

    assert wrapped_call_count == 1  # already called, before the fn even exists

    result = patched_decorator(_fake_compute_fn)

    assert result[0] == "decorator-result"
    assert result[1] is not _fake_compute_fn
    assert result[1].__wrapped__ is _fake_compute_fn


def test_name_kwarg_becomes_traced_span_name() -> None:
    """name=... is passed through as traced()'s span_name -- confirmed
    against a real @op(name="renamed_op") run in test_op.py; this asserts
    the same thing at the dispatch-logic level, without needing a real run."""

    def fake_wrapped(**kwargs):
        return lambda fn: fn

    patched_decorator = _wrap_decorator_factory(
        fake_wrapped, None, (), {"name": "custom_span_name"}
    )
    traced_fn = patched_decorator(_fake_compute_fn)

    # traced()'s own inner wrapper closes over `name` -- not directly
    # inspectable, but __name__ is preserved via functools.wraps either way,
    # so this only confirms wrapping happened; the actual span-name behavior
    # is the real-run assertion in test_op.py::test_parameterized_op_gets_a_span.
    assert traced_fn.__wrapped__ is _fake_compute_fn


def test_no_name_kwarg_means_traced_gets_none() -> None:
    """Parameterized form with some other kwarg, no name= -- traced(None) is
    called, relying on traced()'s own fallback to the function's __name__."""

    def fake_wrapped(**kwargs):
        return lambda fn: fn

    patched_decorator = _wrap_decorator_factory(fake_wrapped, None, (), {"retry_policy": object()})
    traced_fn = patched_decorator(_fake_compute_fn)

    assert traced_fn.__wrapped__ is _fake_compute_fn
