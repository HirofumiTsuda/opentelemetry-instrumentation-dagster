"""Issue #11: every other test's compute function returns cleanly -- nothing has
ever exercised what happens when one raises. `_wrap_decorator_factory` sits
between Dagster and the user's function via `wrapt` plus an extra `traced()`
layer; a regression there that swallows/alters an exception, or corrupts
`result.success`, would go undetected by every other test in this suite.

Not `dagster-otel`'s concern (span creation/error-status recording on the
span) -- the concern here is only whether this package's own wrapping is
transparent to failures. Covers both `_wrap_decorator_factory` dispatch
branches (bare and parameterized), since a wrapping bug could easily be
specific to one."""

import dagster
import pytest
from dagster._core.execution.plan.objects import StepFailureData  # not @public, accepted risk
from dagster_shared.error import SerializableErrorInfo  # not @public, accepted risk
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry.instrumentation.dagster import DagsterInstrumentor


class MyError(ValueError):
    pass


@pytest.fixture
def instrumented():
    instrumentor = DagsterInstrumentor()
    instrumentor.instrument()
    yield instrumentor
    instrumentor.uninstrument()


def _original_exception_cause(
    result: "dagster.ExecuteInProcessResult",
) -> SerializableErrorInfo:
    """The real, user-raised exception's info, unwrapped from Dagster's own
    `DagsterExecutionStepExecutionError` boundary (which wraps every op/asset
    failure regardless of this package's patching) -- confirmed live: the
    STEP_FAILURE event's `error.cls_name` is always
    `DagsterExecutionStepExecutionError`, and the original exception is
    `error.cause`."""
    for event in result.all_events:
        data = event.event_specific_data
        if isinstance(data, StepFailureData):
            assert data.error is not None
            assert data.error.cause is not None
            return data.error.cause
    raise AssertionError("no STEP_FAILURE event found")


def test_bare_op_exception_propagates(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.op
    def failing_op(context) -> None:
        raise MyError("boom")

    @dagster.job
    def my_job() -> None:
        failing_op()

    result = my_job.execute_in_process(raise_on_error=False)
    assert result.success is False

    cause = _original_exception_cause(result)
    assert cause.cls_name == "MyError"
    assert "boom" in cause.message


def test_parameterized_op_exception_propagates(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.op(name="renamed_failing_op")
    def failing_op(context) -> None:
        raise MyError("boom")

    @dagster.job
    def my_job() -> None:
        failing_op()

    result = my_job.execute_in_process(raise_on_error=False)
    assert result.success is False

    cause = _original_exception_cause(result)
    assert cause.cls_name == "MyError"
    assert "boom" in cause.message


def test_bare_asset_exception_propagates(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    @dagster.asset
    def failing_asset(context) -> None:
        raise MyError("boom")

    result = dagster.materialize([failing_asset], raise_on_error=False)
    assert result.success is False

    cause = _original_exception_cause(result)
    assert cause.cls_name == "MyError"
    assert "boom" in cause.message


def test_raise_on_error_still_surfaces_the_real_exception(
    instrumented: DagsterInstrumentor, spans: InMemorySpanExporter
) -> None:
    """The default (raise_on_error=True) path, exercised separately from the
    event-inspection tests above: confirms the wrapping doesn't change what a
    caller relying on the exception actually propagating out of
    execute_in_process() sees.

    Confirmed live: with raise_on_error=True, Dagster re-raises the original
    user exception directly (`raise dagster_user_error.user_exception`,
    `execute_plan.py`), not the `DagsterExecutionStepExecutionError` wrapper
    the STEP_FAILURE event's `error.cls_name` reports -- that wrapper only
    ever surfaces via the event stream (see the other tests in this file),
    never as what's actually raised here."""

    @dagster.op
    def failing_op(context) -> None:
        raise MyError("boom")

    @dagster.job
    def my_job() -> None:
        failing_op()

    with pytest.raises(MyError, match="boom"):
        my_job.execute_in_process()
