"""Verifies the packaging metadata itself, not just the Python logic --
opentelemetry-instrument's whole discovery mechanism (see README.md/docs/
design.md) is entry_points(group="opentelemetry_instrumentor"); a typo or
rename in pyproject.toml's [project.entry-points.opentelemetry_instrumentor]
table would silently break the zero-code-change value proposition with no
other test catching it."""

import importlib.metadata

from opentelemetry.instrumentation.dagster import DagsterInstrumentor


def test_entry_point_resolves_to_dagster_instrumentor() -> None:
    eps = importlib.metadata.entry_points(group="opentelemetry_instrumentor")
    assert eps["dagster"].load() is DagsterInstrumentor
