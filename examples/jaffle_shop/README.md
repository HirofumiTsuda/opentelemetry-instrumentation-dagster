# jaffle_shop (dev fixture)

A minimal, hand-written dbt project (`raw_customers` seed -> `stg_customers`
staging model -> `customers` mart model, with `not_null`/`unique` data tests
on each) used to verify this package's `@dbt_assets` auto-instrumentation
against a real dbt project instead of the hand-constructed
`AssetsDefinition`s in `tests/test_dbt_assets.py`. See
[Issue #28](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/28).

## Attribution

Copied from [`dagster-otel`](https://github.com/HirofumiTsuda/dagster-otel)'s
own `examples/jaffle_shop` (same author, same license, not vendored from
anywhere else). That project's own attribution note applies here too: the
naming and shape is modeled after dbt Labs' tutorial project,
[`dbt-labs/jaffle_shop_duckdb`](https://github.com/dbt-labs/jaffle_shop_duckdb)
(the `duckdb` branch) -- no files vendored or copied from it directly, trimmed
to the one dependency chain needed here.

## Database

Target is DuckDB, file-based (generated on first `dbt build`/materialization,
gitignored). No extra services required beyond the local Jaeger this repo's
`docker-compose.yaml` already provides.
