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

1. Patch `dagster.asset` / `dagster.op` (public, stable decorator factories)
   so that each, when called, first wraps the incoming compute function with
   `dagster_otel.traced()` before handing it to the real decorator.
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
signature doesn't even match. `asset`/`op` must be the only patch targets;
`graph_asset` needs to pass through completely unpatched. (Tracing a
`graph_asset`'s actual execution already works today, for free, by patching
each of the individual `@op`s it composes -- those really do run with a
`context` at execution time.)

## Try it (once it exists)

```sh
pip install opentelemetry-instrumentation-dagster
```

Nothing to configure beyond what `dagster-otel` itself needs (standard
`OTEL_*` env vars) -- no `@traced()` calls anywhere in your own code.

## License

MIT
