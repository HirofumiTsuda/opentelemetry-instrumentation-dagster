# opentelemetry-instrumentation-dagster

Auto-instrumentation for Dagster ops/assets -- zero-code tracing, no
`@traced()` decorator required. The opt-in companion to
[`dagster-otel`](https://github.com/HirofumiTsuda/dagster-otel), not a
replacement for it: `dagster-otel` does the actual span creation, this
package's only job is applying it automatically to every `@op`/`@asset`/
`@multi_asset` (and `@dbt_assets`) by patching Dagster's own decorators,
rather than you writing `@traced()` under each one yourself. See
[docs/design.md](docs/design.md) for why this is a separate package instead
of a `dagster-otel` feature, and the full investigation behind how the patch
works.

**Status: early -- `@op`/`@asset`/`@multi_asset` (and `@dbt_assets`, which
rides along on `@multi_asset` for free) are patched and verified against
real Dagster execution, including genuine cross-process execution under both
`multiprocess` and `k8s_job_executor` (a real `kind` cluster,
`dev/kubernetes/`) -- both exported to a real Jaeger. `@graph_asset`
deliberately excluded. Not yet released to PyPI.**

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [What's covered](#whats-covered)
- [Configuration](#configuration)
- [`multiprocess`/`k8s_job_executor`](#multiprocessk8s_job_executor)
- [Compatibility](#compatibility)
- [Why a separate package](#why-a-separate-package)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
pip install opentelemetry-instrumentation-dagster
```

## Usage

No `@traced()` calls anywhere in your own code -- run your usual Dagster
command through the `opentelemetry-instrument` launcher (installed as part
of this package's `opentelemetry-instrumentation` dependency) instead of
running it directly:

```sh
OTEL_SERVICE_NAME=my_pipeline \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
opentelemetry-instrument dagster dev -f definitions.py
```

That's the entire setup. Every `@op`/`@asset`/`@multi_asset`/`@dbt_assets`
in `definitions.py` gets a span automatically, with no decorator, no import,
no change to the file at all:

```python
from dagster import asset, job, op

@op
def upstream_op(context) -> int:
    return 1

@op
def downstream_op(context, x: int) -> int:
    return x + 1

@asset
def my_asset(context) -> None:
    ...

@job
def my_job():
    downstream_op(upstream_op())
```

The launcher works the same way in front of `dagster job execute`,
`dagster-webserver`, `dagster-daemon`, or any other Dagster entry point --
it's a drop-in prefix, not something specific to `dagster dev`.

## What's covered

| Decorator | Status |
| --- | --- |
| `@op` | ✅ Patched -- bare and `@op(name=...)` forms |
| `@asset` | ✅ Patched -- bare and `@asset(name=...)` forms |
| `@multi_asset` | ✅ Patched |
| `@dbt_assets` (`dagster_dbt`) | ✅ Covered for free -- it calls `multi_asset` internally, see [docs/design.md](docs/design.md). One span per dbt run (not `dagster-otel`'s finer per-model `@traced_dbt()` granularity) -- [Issue #6](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/6) tracks verifying this against a real dbt project end to end. |
| `@graph_asset` | ⬜ Deliberately not patched -- its decorated function never receives a runtime `context` at all, so `@traced()` doesn't apply to it. Tracing the ops it composes (which already works, no changes needed) already covers everything that actually executes. See [docs/design.md](docs/design.md). |

## Configuration

Same standard OTel environment variables `dagster-otel` itself reads --
nothing this package adds on top:

| Variable | Purpose |
| --- | --- |
| `OTEL_SERVICE_NAME` | Names your service in the trace backend. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` (or `..._TRACES_ENDPOINT`) | Where to send spans (e.g. `http://localhost:4317`). Required -- without one of these set, no real exporter is attached at all. |
| `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `..._PROTOCOL` | Transport to export over: `grpc` (default) or `http/protobuf`. |
| `OTEL_SDK_DISABLED` | Set to `true` to force no export regardless of the endpoint vars above. |

See [`dagster-otel`'s own README](https://github.com/HirofumiTsuda/dagster-otel#configuration)
for the full list and what each one actually does underneath -- this package
doesn't wrap or reinterpret any of it, just triggers the same `traced()`
that reads these itself.

## `multiprocess`/`k8s_job_executor`

Each step in a `multiprocess`-executed run runs in its own, freshly spawned
Python interpreter -- the `opentelemetry-instrument` launcher above already
handles this correctly (verified against a real run, see
[docs/design.md](docs/design.md)), no extra setup needed.

`k8s_job_executor` is different: each step becomes a genuinely separate
Kubernetes Pod, and the launcher's usual mechanism can't propagate into a
brand new container the way it does into a spawned OS subprocess. This
needs the instrumentation baked into the container image itself instead --
see [`dev/kubernetes/`](dev/kubernetes/) for a complete, verified-against-a-
real-cluster example (`Dockerfile`, manifests, and why), and
[docs/design.md](docs/design.md) for the reasoning.

## Compatibility

Depends on [`dagster-otel`](https://github.com/HirofumiTsuda/dagster-otel)
and `dagster >= 1.5`, same floor as that project. Not independently
version-matrix-tested beyond what `dagster-otel` itself covers -- if you hit
an incompatibility, [open an issue](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/new).

## Why a separate package

`dagster-otel`'s whole pitch is tracing *without* monkeypatching Dagster
internals and *without* taking ownership of your op/asset definitions away
from you. Auto-instrumentation is the opposite trade: zero code changes, in
exchange for some framework patching and losing that per-function
visibility. Both are legitimate, but they're different products for
different people -- same split the OpenTelemetry Python ecosystem itself
uses (`opentelemetry-instrumentation-flask`, `-django`, etc. are all
separate packages from the manual API/SDK). See
[docs/design.md](docs/design.md) for the full reasoning.

## Contributing

Issues and PRs welcome -- [open an issue](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/new)
for bugs, missing coverage, or a backend that doesn't work as expected.

## License

MIT
