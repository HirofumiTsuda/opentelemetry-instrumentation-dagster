"""Auto-instrumentation for Dagster ops/assets.

Not implemented yet -- this is a scaffold. See README.md for the design this is
meant to follow: patch dagster.asset/op/multi_asset (public decorator factories,
not private internals) so each already applies dagster_otel.traced() to the
compute function it wraps, before Dagster ever builds the resulting
AssetsDefinition/OpDefinition. Must run before the user's Definitions module
imports those names -- same timing constraint every other
opentelemetry-instrumentation-* package has, hence patching in _instrument()
rather than at import time here.
"""

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

from .version import __version__

__all__ = ["DagsterInstrumentor", "__version__"]


class DagsterInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return ("dagster >= 1.5",)

    def _instrument(self, **kwargs):
        raise NotImplementedError

    def _uninstrument(self, **kwargs):
        raise NotImplementedError
