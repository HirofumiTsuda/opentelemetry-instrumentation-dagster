# opentelemetry-instrumentation-dagster

[![PyPI](https://img.shields.io/pypi/v/opentelemetry-instrumentation-dagster)](https://pypi.org/project/opentelemetry-instrumentation-dagster/)
[![Python versions](https://img.shields.io/pypi/pyversions/opentelemetry-instrumentation-dagster)](https://pypi.org/project/opentelemetry-instrumentation-dagster/)
[![CI](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/actions/workflows/ci.yml/badge.svg)](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/actions/workflows/ci.yml)
[![CodeQL](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/actions/workflows/codeql.yml/badge.svg)](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/github/license/HirofumiTsuda/opentelemetry-instrumentation-dagster)](LICENSE)

Automatic tracing for [Dagster](https://dagster.io/) pipelines: `pip
install`, run your pipeline through the `opentelemetry-instrument` launcher,
and every `@op`/`@asset`/`@multi_asset`/`@asset_check`/`@dbt_assets` gets a
span. No `@traced()` decorators, no code changes, no imports in your own
pipeline files at all.

Built on [`dagster-otel`](https://github.com/HirofumiTsuda/dagster-otel)
(same author) -- that project does the actual span creation via an explicit
`@traced()` decorator; this one's only job is applying it automatically
instead. See [docs/design.md](docs/design.md) for why they're two separate
packages rather than one.

<details>
<summary><strong>Status</strong> (early, but verified against real infrastructure -- click to expand)</summary>

<br>

`@op`/`@asset`/`@multi_asset` (and `@dbt_assets`, which rides along on
`@multi_asset` for free) are patched and verified against real Dagster
execution, including genuine cross-process execution under both
`multiprocess` and `k8s_job_executor` (a real `kind` cluster,
`dev/kubernetes/`) -- both exported to a real Jaeger. `@asset_check` is also
patched (same mechanism, verified against a real `materialize()` run), though
not independently re-verified under `multiprocess`/`k8s_job_executor` yet.
`@graph_asset` deliberately excluded (see [What's covered](#whats-covered)).
`@dbt_assets` not yet verified against a real dbt project end to end --
[Issue #6](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/6).

</details>

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
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

## Quick Start

`docker compose up -d` brings up a local Jaeger (no other setup) so you can
see a real trace land within a couple of minutes.
[`examples/definitions.py`](examples/definitions.py) is a plain Dagster job
and asset -- zero `@traced()` calls, zero `dagster_otel`/`opentelemetry`
imports, nothing at all:

```sh
git clone https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster
cd opentelemetry-instrumentation-dagster
docker compose up -d

pip install opentelemetry-instrumentation-dagster
OTEL_SERVICE_NAME=quickstart \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
opentelemetry-instrument dagster asset materialize -f examples/definitions.py --select example_asset
```

Open http://localhost:16686 (Jaeger's UI), select the `quickstart` service,
and there's the span -- `example_asset`, from a file that never imported
this package or `dagster-otel` at all.

Teardown: `docker compose down`.

## Usage

Same idea against your own pipeline: run your usual Dagster command through
the `opentelemetry-instrument` launcher instead of running it directly.

```sh
OTEL_SERVICE_NAME=my_pipeline \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
opentelemetry-instrument dagster dev -f definitions.py
```

That's the entire setup. Every `@op`/`@asset`/`@multi_asset`/`@asset_check`/
`@dbt_assets` in `definitions.py` gets a span automatically, with no
decorator, no import, no change to the file at all:

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
| `@asset_check` | ✅ Patched -- `@asset_check(asset=...)` and `@asset_check(asset=..., name=...)` forms. Requires `dagster-otel >= 0.4.0` (its `AssetCheckExecutionContext` support). |
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
no `opentelemetry-instrument` prefix anywhere, because nothing in your own
config controls the command `k8s_job_executor` constructs internally for
each step Pod.

### Using this with `k8s_job_executor`

In your own Dockerfile, after installing this package, copy
`opentelemetry-instrumentation`'s real `sitecustomize.py` into your venv's
site-packages root:

```dockerfile
RUN site_packages="$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')" && \
    cp "$site_packages/opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py" \
       "$site_packages/sitecustomize.py"
```

Python auto-imports any module literally named `sitecustomize` found
directly in site-packages at interpreter startup -- so this alone is
enough, in every container built from that image, including every step Pod
`k8s_job_executor` launches from it. No `PYTHONPATH`, no `.pth` file, no
launcher prefix anywhere in your run config or Dockerfile `CMD`.

(If your build process makes a `sysconfig`-based path awkward -- e.g. a
venv at a path you already know ahead of time -- `find /path/to/venv
-maxdepth 4 -type d -name site-packages` works just as well. Setting
`PYTHONPATH` explicitly via `k8s_job_executor`'s `env_vars` run config is
an alternative to copying the file at all, if that fits your deployment
better.)

[`dev/kubernetes/`](dev/kubernetes/) is a complete, real-cluster-verified
example of this exact pattern (`Dockerfile`, `kind` manifests, RBAC) --
written as this project's own verification harness, not a
copy-paste-ready deployment template, but the Dockerfile's approach is the
same one described above. See [docs/design.md](docs/design.md) for the
full reasoning and the negative-control verification.

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
