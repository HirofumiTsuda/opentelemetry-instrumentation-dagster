# Design notes

The investigation log behind this package's design decisions -- what was
tried, what was checked against real Dagster/OTel source, what was verified
against real infrastructure. README.md stays focused on installation/usage;
this is where the reasoning and evidence live.

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

## How the patch works

The naive approach -- reach into an already-built `AssetsDefinition` and swap
its compute function -- means touching non-public attributes of an object
that was never meant to be mutated after construction. Instead:

1. Patch `dagster.asset` / `dagster.op` / `dagster.multi_asset` /
   `dagster.asset_check` (public, stable decorator factories) so that each,
   when called, first wraps the incoming compute function with
   `dagster_otel.traced()` before handing it to the real decorator.
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

**Verified against a real run, not just reasoned through.** A job with two
plain `@op`s (no `@traced()` anywhere in the file), `executor_def=
multiprocess_executor`, launched via `execute_job()` (not `execute_in_process()`
-- confirmed the latter always runs everything in one process regardless of
the configured executor, so it can't exercise this at all) under `opentelemetry-
instrument python job.py`, with a real Jaeger as the OTLP target:

- Dagster's own log confirmed genuinely separate PIDs for each step (parent
  `2363506`, `upstream_op` subprocess `2363763`, `downstream_op` subprocess
  `2363915`).
- Jaeger received both spans, correctly parented (`downstream_op` a
  `CHILD_OF` `upstream_op`) -- the cross-process trace-context propagation
  is entirely `dagster-otel`'s own existing mechanism (via run storage),
  which needed no help from this package once spans were being created at
  all.
- **Negative control**: the identical job run again, same env vars, minus
  `opentelemetry-instrument` (plain `python job.py`) -- zero traces reached
  Jaeger. Confirms the launcher is the actual load-bearing mechanism here,
  not some coincidental side effect.

**This breaks under `k8s_job_executor`.** Checked `dagster_k8s/executor.py`
(dagster-io/dagster): each step becomes a genuinely separate Kubernetes Job/
Pod, and the env vars forwarded into it are an explicit, fixed list --
`execute_step_args.get_command_env()` (things like `DAGSTER_HOME`) plus
`DAGSTER_RUN_JOB_NAME`/`DAGSTER_RUN_STEP_KEY`. `PYTHONPATH` isn't among them,
and there's no OS-level environment inheritance between the orchestrating
process and a brand new Pod's container the way there is for a `spawn`ed
local subprocess. The `sitecustomize.py` trick's dynamic propagation doesn't
apply here at all.

The practical fix for k8s: don't rely on propagation -- make
`sitecustomize.py` reachable *statically*. Python auto-imports any module
literally named `sitecustomize` found directly in site-packages at
interpreter startup, so `dev/kubernetes/Dockerfile` builds the image with a
venv at a path it chooses (via `uv`, not plain `pip install` -- see that
file's own comments), finds `opentelemetry-instrumentation`'s real
`sitecustomize.py` inside it, and copies that one file to site-packages'
own root. No `PYTHONPATH` or `.pth` file needed -- just the plain import
mechanism every Python interpreter already has, pointed at a file that's
already there. Setting `PYTHONPATH` explicitly in `k8s_job_executor`'s
`env_vars` run config would also work, if this doesn't fit a given
deployment's build process. This is the same shape as how the OpenTelemetry
Operator's actual Kubernetes auto-instrumentation feature works (a mutating
webhook injects `PYTHONPATH` directly into the Pod spec) -- static injection
into the Pod spec, not dynamic process inheritance, is the normal pattern
for k8s specifically.

**Verified against a real cluster, not just reasoned through** --
`dev/kubernetes/` (a real `kind` cluster, Postgres-backed run storage,
`k8s_job_executor`, a real Jaeger, adapted from `dagster-otel`'s own
equivalent setup): a two-op job with **zero `@traced()` calls anywhere**,
`sitecustomize.py` copied into the image per above, no
`opentelemetry-instrument` wrapper on the runner pod's command either (the
copied file covers it too, same as every step pod). Result: `kubectl get
pods` showed the runner pod plus two separate `dagster-step-<hash>` pods,
each its own Kubernetes Job; Jaeger received both spans, correctly parented
(`downstream_op` a `CHILD_OF` `upstream_op`). See `dev/kubernetes/README.md`
to reproduce.

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
correctness here. (The real implementation additionally passes a `name=...`
override through to `traced(span_name=...)` when given -- confirmed against
a real `@op(name="renamed_op")` run that `traced()`'s own default otherwise
produces a span named after the Python function, not the name Dagster
actually gives the op/step.)

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
project/manifest -- see [Issue #6](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/6)
for that; this only proves the reference-sharing mechanism itself).

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

## `asset_check` patch (Issue #16)

Before this, asset checks ran with zero tracing even in a fully
auto-instrumented pipeline -- a real, silent coverage gap for anyone using
Dagster's asset-checks feature, since only `op`/`asset`/`multi_asset` were
patched.

Checked directly against the installed `dagster` package:
`dagster.asset_check` (`dagster/_core/definitions/decorators/
asset_check_decorator.py`) is `*, asset: ..., name: str | None = None, ...`
-- keyword-only, same shape as `multi_asset`, so it only ever exercises
`_wrap_decorator_factory`'s existing parameterized branch. No new dispatch
logic needed -- just a fourth `wrapt.wrap_function_wrapper("dagster",
"asset_check", _wrap_decorator_factory)` (and matching `unwrap`) in
`_instrument()`/`_uninstrument()`.

Its decorated function receives a runtime `AssetCheckExecutionContext` --
a genuinely different shape from `OpExecutionContext`/
`AssetExecutionContext` (no `.job_name`, no `.selected_asset_keys`, see
`dagster-otel`'s own Issue #72 writeup). `traced()` only gained support for
that context type in `dagster-otel` 0.4.0, so this package's dependency was
bumped to `dagster-otel >= 0.4.0` alongside this patch -- applying `traced()`
to a real `@asset_check` function via any older `dagster-otel` crashes with
`AttributeError: 'AssetCheckExecutionContext' object has no attribute
'job_name'`.

Verified two ways:

- `tests/test_asset_check.py`: a real `dagster.materialize()` run (via
  `DagsterInstrumentor().instrument()` directly) with an
  `@asset_check(asset=...)` alongside its checked `@asset`, both bare-name
  and `name=...` forms, confirming a span lands for the check itself (not
  just the asset it checks).
- The actual zero-code path, end to end: a script with no `instrument()`
  call and no `@traced()` anywhere, run under `opentelemetry-instrument
  python ...`. `dagster.asset_check` was already the wrapt-patched function
  by the time the script's own `import dagster` ran (confirmed by
  inspecting `dagster.asset_check.__wrapped__`), and materializing an
  `@asset` plus an `@asset_check(asset=...)` against it produced spans for
  both, with no code in the script referencing this package or
  `dagster_otel` at all -- the same entry-point/`sitecustomize.py`
  mechanism the other three decorators already rely on (see "How the patch
  works" above), just confirmed here too rather than assumed to extend.

Not yet independently re-verified under `multiprocess`/`k8s_job_executor`
specifically -- same generic patch mechanism already verified there for the
other three decorators, but no dedicated real-cluster run for `asset_check`.

## `@dbt_assets` verified against a real dbt project (Issue #6/#28)

`tests/test_dbt_assets.py` only ever proved the *mechanism* this package's
dbt claim depends on -- that `dagster_dbt.asset_decorator.multi_asset is
dagster.multi_asset` after instrumenting, using a hand-constructed
`AssetsDefinition`. Nothing confirmed a real `@dbt_assets` function, built
from an actual `manifest.json` (real `dbt parse`, not hand-rolled), produces
a working span end to end.

`examples/jaffle_shop/` (Issue #28) is the same small hand-written dbt
project as `dagster-otel`'s own `examples/jaffle_shop` (seed -> staging
model -> mart model, copied -- same author, same license), and
`examples/dbt_workspace/definitions.py` wraps its `@dbt_assets` function
with **zero `@traced()`/`@traced_dbt()` calls and no `dagster_otel`/
`opentelemetry` imports at all** -- the entire point being that this
package's patching alone should be enough.

Verified live, `opentelemetry-instrument dagster asset materialize -f
examples/dbt_workspace/definitions.py --select '*'` against a real Jaeger:

- A real span named `jaffle_shop_dbt_assets` landed, confirming the
  `AssetsDefinition` `dagster_dbt.dbt_assets` actually produces really does
  get a `traced()`-wrapped compute function when built through the patched
  `multi_asset` -- not just that the two names are `is`-identical (the
  narrower thing the existing white-box test already covered).
- One span per dbt run, ~10-12s duration each across three separate runs --
  not `dagster-otel`'s finer per-model/per-test `traced_dbt()` granularity
  (documented, known gap, tracked separately as Issue #14 -- out of scope
  here, see README's "What's covered" table).
- `@dbt_assets(..., name="renamed_jaffle_shop")`'s `name=` kwarg (passed
  through to `multi_asset`, see `_wrap_decorator_factory`'s `span_name`
  handling) produced a span literally named `renamed_jaffle_shop`, not the
  Python function's own name -- confirmed against a real dbt run, not just
  the hand-picked test value `tests/test_op.py`'s equivalent `@op(name=...)`
  check already used.

One gotcha hit while building the fixture, worth recording since it's easy
to reproduce by reasoning alone: generating the manifest via `dbt parse
--project-dir examples/jaffle_shop --profiles-dir examples/jaffle_shop`
from the repo root (rather than `cd examples/jaffle_shop && dbt parse
--profiles-dir .`, what `dagster-otel`'s own README instructs) bakes seed
file paths into the manifest relative to the *invocation* cwd, not the
project dir -- `dbt build` then fails at materialize time with `IO Error:
No files found that match the pattern "examples/jaffle_shop/seeds/
raw_customers.csv"` (doubled path, since `DbtCliResource` invokes dbt with
`cwd` set to the project dir itself). Regenerating the manifest by `cd`-ing
into the project dir first (matching the instructions exactly) fixed it.
