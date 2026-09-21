"""Quickstart example -- see the top-level README.md's Quick Start section.

Deliberately has zero `@traced()` calls, zero `dagster_otel`/`opentelemetry`
imports, nothing at all -- that's the entire point of this package. Every
`@op`/`@asset`/`@multi_asset` here gets a span automatically, purely from
being run under the `opentelemetry-instrument` launcher.
"""

from dagster import Definitions, asset, job, op


@op
def upstream_op(context) -> int:
    return 1


@op
def downstream_op(context, x: int) -> int:
    return x + 1


@job
def example_job():
    downstream_op(upstream_op())


@asset
def example_asset(context) -> int:
    return 1


defs = Definitions(jobs=[example_job], assets=[example_asset])
