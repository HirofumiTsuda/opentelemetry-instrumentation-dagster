# opentelemetry-instrumentation-dagster

**Status: scaffold only, nothing implemented yet.**

Auto-instrumentation for Dagster ops/assets -- zero-code tracing, no
`@traced()` decorator required. The opt-in companion to
[`dagster-otel`](https://github.com/HirofumiTsuda/dagster-otel), not a
replacement for it.

## Why a separate package, not a `dagster-otel` feature

`dagster-otel`'s whole pitch is tracing *without* monkeypatching Dagster
internals and *without* taking ownership of your op/asset definitions away
from you -- you stack `@traced()` under `@op`/`@asset` yourself, explicitly.
Auto-instrumentation is the opposite trade: zero code changes, in exchange for
some framework patching and losing that per-function visibility. Both are
legitimate, but they're different products for different people, and folding
the second into the first would quietly undermine what `dagster-otel` already
promises. Same split the OpenTelemetry Python ecosystem itself uses --
`opentelemetry-instrumentation-flask`, `-django`, etc. are all separate
packages from the manual API/SDK.

## Design (not yet built)

The naive approach -- reach into an already-built `AssetsDefinition` and swap
its compute function -- means touching non-public attributes of an object
that was never meant to be mutated after construction. Instead:

1. Patch `dagster.asset` / `dagster.op` / `dagster.multi_asset` (public,
   stable decorator factories) so that each, when called, first wraps the
   incoming compute function with `dagster_otel.traced()` before handing it to
   the real decorator.
2. This relies on decorator-application order: if the compute function is
   already wrapped *before* `@asset`/`@op` builds the `AssetsDefinition`/
   `OpDefinition`, no post-hoc mutation is ever needed -- same trick
   `dagster-otel`'s own manual `@traced()` already relies on, just applied by
   this package instead of by the user.
3. **Timing matters**: the patch has to be in place before the user's
   `Definitions` module does `from dagster import asset`, or it's patching a
   name nothing still refers to. Real `opentelemetry-instrumentation-*`
   packages solve this with a launcher (`opentelemetry-instrument python
   app.py`) that patches first, then imports the target app -- this package
   will need the same, or an equivalent (e.g. requiring `dagster.yaml`/the
   workspace loader to import this package before the `Definitions` module).

## Try it (once it exists)

```sh
pip install opentelemetry-instrumentation-dagster
```

Nothing to configure beyond what `dagster-otel` itself needs (standard
`OTEL_*` env vars) -- no `@traced()` calls anywhere in your own code.

## License

MIT
