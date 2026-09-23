"""Real-dbt-project example for Issue #6/#28 -- see examples/jaffle_shop/README.md.

Deliberately has zero `@traced()`/`@traced_dbt()` calls and no
`dagster_otel`/`opentelemetry` imports at all, same as examples/definitions.py:
`dagster_dbt.dbt_assets` calls `dagster.multi_asset(...)` internally, so
patching `multi_asset` (this package's job) covers it transitively -- that's
the whole thing this example exists to verify against a real dbt project
rather than a hand-constructed AssetsDefinition.

Run with (see the top-level README.md's Quick Start for the docker-compose
Jaeger setup):
    DAGSTER_HOME=... OTEL_SERVICE_NAME=jaffle_shop_example \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    opentelemetry-instrument dagster asset materialize \
        -f examples/dbt_workspace/definitions.py --select '*'
"""

from pathlib import Path

from dagster import AssetExecutionContext, Definitions
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

jaffle_shop_project = DbtProject(project_dir=Path(__file__).parent.parent / "jaffle_shop")
jaffle_shop_project.prepare_if_dev()


@dbt_assets(manifest=jaffle_shop_project.manifest_path)
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[jaffle_shop_dbt_assets],
    resources={"dbt": DbtCliResource(project_dir=jaffle_shop_project)},
)
