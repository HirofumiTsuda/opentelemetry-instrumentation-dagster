# opentelemetry-instrumentation-dagster

**Status: early -- `@op`/`@asset`/`@multi_asset` (and `@dbt_assets`, which
rides along on `@multi_asset` for free) are patched and verified against
real Dagster execution. `@graph_asset` deliberately excluded. Not yet
released to PyPI; not yet verified under `multiprocess`/`k8s_job_executor`
timing.**

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
   incoming compute function with `dagster_otel.traced()` before handing it
   to the real decorator.
2. This relies on decorator-application order: if the compute function is
   already wrapped *before* `@asset`/`@op` builds the `AssetsDefinition`/
   `OpDefinition`, no post-hoc mutation is ever needed -- same trick
   `dagster-otel`'s own manual `@traced()` already relies on, just applied by
   this package instead of by the user.
3. **Timing matters**: the patch has to be in place before the user's
   `Definitions` module does `from dagster import asset`, or it's patching a
   name nothing still refers to. No launcher needs to be built here, though --
   `opentelemetry-instrument` (from the `opentelemetry-instrumentation`
   package this depends on) already *is* that launcher, generically, for any
   registered instrumentor. `_load_instrumentors()` just iterates
   `entry_points(group="opentelemetry_instrumentor")` and calls
   `.instrument()` on each -- exactly the group this package's `pyproject.toml`
   registers `DagsterInstrumentor` under. So the only thing to actually build
   is `DagsterInstrumentor._instrument()` itself; running
   `opentelemetry-instrument dagster dev -f definitions.py` is enough to pick
   it up, timing included (see below for why that also covers `multiprocess`
   subprocesses, and why it doesn't for `k8s_job_executor`).

### Timing gets harder with `multiprocess`/`k8s_job_executor`

`dagster-otel` already established that each step in a `multiprocess`-executed
run runs in its own process (see its own README/design.md) -- what that means
here is Dagster's `MultiprocessExecutor` defaults to
`start_method="spawn"` (checked in `dagster/_core/executor/multiprocess.py`),
so each step is a *fresh* interpreter that re-imports the `Definitions`
module from scratch. A patch applied only in the original `dagster dev`
process doesn't carry over on its own.

It turns out `opentelemetry-instrument` already solves exactly this, just not
obviously: it doesn't patch things directly in-process. Instead it inserts
its own directory (containing a two-line `sitecustomize.py`) at the front of
the `PYTHONPATH` environment variable, then `execl()`s into the target
command. Python auto-imports any module named `sitecustomize` found on
`sys.path` at interpreter startup -- and since `PYTHONPATH` is an environment
variable, every child process that inherits the environment (which `spawn`
does, by default) re-triggers the same `sitecustomize.py` → re-applies every
registered instrumentor, independently, before that child re-imports
`Definitions`. No explicit re-wrapping needed per subprocess -- it rides on
Python's own site-import mechanism plus ordinary environment inheritance.

**This breaks under `k8s_job_executor`.** Checked `dagster_k8s/executor.py`
(dagster-io/dagster): each step becomes a genuinely separate Kubernetes Job/
Pod, and the env vars forwarded into it are an explicit, fixed list --
`execute_step_args.get_command_env()` (things like `DAGSTER_HOME`) plus
`DAGSTER_RUN_JOB_NAME`/`DAGSTER_RUN_STEP_KEY`. `PYTHONPATH` isn't among them,
and there's no OS-level environment inheritance between the orchestrating
process and a brand new Pod's container the way there is for a `spawn`ed
local subprocess. The `sitecustomize.py` trick's dynamic propagation doesn't
apply here at all.

The practical fix for k8s: don't rely on propagation -- set `PYTHONPATH`
*statically*, since the package is already `pip install`ed into the step
container's image and its `sitecustomize.py` location inside site-packages is
fixed and known ahead of time. Either bake `ENV PYTHONPATH=...` into the
Dockerfile, or add `PYTHONPATH` explicitly to `k8s_job_executor`'s `env_vars`
run config. This is the same shape as how the OpenTelemetry Operator's actual
Kubernetes auto-instrumentation feature works (a mutating webhook injects
`PYTHONPATH` directly into the Pod spec) -- static injection into the Pod
spec, not dynamic process inheritance, is the normal pattern for k8s
specifically.

### Why patch the decorators, not the actual invoke point

`opentelemetry-instrumentation-click` (see `opentelemetry-python-contrib`)
patches `click.core.Command.invoke` -- the method that runs when a command
actually *executes* -- rather than the `@click.command()` decorator itself,
sidestepping the bare-vs-parameterized-decorator problem entirely. Checked
whether Dagster has an equivalent: it does, `dagster._core.execution.plan.
compute_generator.invoke_compute_fn`, the single place that actually calls
`fn(context, **kwargs)`. But unlike `click.core.Command` (genuinely public,
re-exported as `click.Command`), this lives under Dagster's own `_core`
namespace -- never re-exported from the top-level `dagster` package, and
every internal Dagster package is underscore-prefixed the same way. Patching
it would be exactly the private-internals dependency `dagster-otel` was
designed to avoid. So: patch the public decorators after all, bare/
parameterized complexity included.

### Sketch of the decorator wrapper

```python
import wrapt
from dagster_otel import traced

def _wrap_decorator_factory(wrapped, instance, args, kwargs):
    # bare form: @asset -- first positional arg is the compute function itself
    if args and callable(args[0]) and not kwargs:
        fn, *rest = args
        return wrapped(traced()(fn), *rest, **kwargs)

    # parameterized form: @asset(name=...) -- wrapped(**kwargs) returns a
    # decorator; wrap *that* so traced() gets applied when it's later applied
    # to the actual function
    real_decorator = wrapped(*args, **kwargs)

    def patched_decorator(fn):
        return real_decorator(traced()(fn))

    return patched_decorator

# registered via wrapt.wrap_function_wrapper("dagster", "asset", ...) and
# ("dagster", "op", ...) in _instrument()
```

Needs verifying against a real Dagster run before trusting it, same as
everything else in `dagster-otel`'s own history -- types alone don't confirm
correctness here.

### `graph_asset` is out of scope -- deliberately, not by oversight

`@graph_asset`'s decorated function is a *composition* function, called once
at definition time to wire up which `@op`s depend on which -- it never
receives a runtime `ExecutionContext` at all (see the decorator's own
docstring example: `def slack_files_table(): return store_files(fetch_files_
from_slack())`, no `context` param). `traced()` assumes a `(context, ...)`
runtime call; applying it to a `graph_asset`'s compose function would be
wrong, not just unnecessary -- there's no per-run invocation to wrap, and the
signature doesn't even match. `asset`/`op`/`multi_asset` are the patch
targets; `graph_asset` needs to pass through completely unpatched. (Tracing a
`graph_asset`'s actual execution already works today, for free, by patching
each of the individual `@op`s it composes -- those really do run with a
`context` at execution time.)

### `multi_asset` covers `@dbt_assets` for free -- but only at `traced()`'s granularity

`dagster_dbt.dbt_assets` (checked `dagster_dbt/asset_decorator.py`) isn't its
own independent code path -- it just calls `dagster.multi_asset(specs=...,
...)` and returns whatever that returns:

```python
# dagster_dbt/asset_decorator.py
return multi_asset(
    name=name,
    specs=specs,
    ...
)
```

So patching `dagster.multi_asset` (needed anyway -- it's keyword-only,
`def multi_asset(*, outs=None, ...)`, no bare form, so it only ever exercises
the parameterized branch of `_wrap_decorator_factory`, no extra dispatch
logic needed) transitively covers `@dbt_assets` too, with zero dbt-specific
code in this package. `dagster_dbt` calling the patched `multi_asset` "just
works" the same way any other caller of a patched function does -- **with
one real caveat**: `dagster_dbt/asset_decorator.py` does `from dagster
import ... multi_asset ...` at its own module import time, a one-time name
binding, not a live link back to `dagster`'s attribute. So this only works
if `dagster_dbt` gets imported *after* `instrument()` has already run (the
supported order, since `opentelemetry-instrument`'s `sitecustomize.py` runs
before any user code imports anything) -- if `dagster_dbt` is somehow
already imported first, its own `multi_asset` reference is permanently
frozen to the pre-patch function. Both directions are verified directly, not
just asserted, in `tests/test_dbt_assets.py` (not against a real dbt
project/manifest -- `dagster-otel`'s own test suite already covers real dbt
materialization end to end; this only needs to prove the reference-sharing
mechanism itself).

The remaining gap: `dagster-otel` has a dedicated `traced_dbt()` (not just
`traced()`) for exactly this case, which additionally opens a child span per
dbt node (model/seed/test) -- real per-node granularity, not one span for
the whole dbt run. Auto-applying plain `traced()` via the `multi_asset`
patch gives every dbt run exactly one span, same as any other multi_asset --
correct, but coarser than what a `dagster-otel` user gets by writing
`@traced_dbt()` explicitly. Closing that gap would need this package to
detect "this `multi_asset` call is actually a `@dbt_assets` call" (e.g. a
`manifest` kwarg, or checking the call site) and dispatch to `traced_dbt()`
instead -- not yet designed.

## Try it (once it exists)

```sh
pip install opentelemetry-instrumentation-dagster
```

Nothing to configure beyond what `dagster-otel` itself needs (standard
`OTEL_*` env vars) -- no `@traced()` calls anywhere in your own code.

## License

MIT
