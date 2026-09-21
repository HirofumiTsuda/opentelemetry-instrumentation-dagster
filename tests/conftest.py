import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry import trace


@pytest.fixture(scope="session", autouse=True)
def _otel_test_provider() -> InMemorySpanExporter:
    """One TracerProvider(InMemorySpanExporter) for the whole test session --
    set_tracer_provider() only takes effect on the first call per process, same
    reasoning as dagster-otel's own conftest.py."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture
def spans(_otel_test_provider: InMemorySpanExporter) -> InMemorySpanExporter:
    _otel_test_provider.clear()
    return _otel_test_provider
