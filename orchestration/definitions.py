import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, Definitions, asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt_stib"
GOLD_TABLES = ["fct_collection_health", "fct_line_speeds", "fct_headway_regularity"]

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()


def run_module(context: AssetExecutionContext, module: str) -> None:
    """Lance un job Spark existant dans un sous-processus, depuis la racine du projet."""
    context.log.info(f"python -m {module}")
    subprocess.run([sys.executable, "-m", module], cwd=PROJECT_ROOT, check=True)


@asset(group_name="bronze")
def bronze_positions(context: AssetExecutionContext) -> None:
    run_module(context, "src.spark_jobs.bronze_ingestion")


@asset(deps=[bronze_positions], group_name="silver")
def silver_cleaned(context: AssetExecutionContext) -> None:
    run_module(context, "src.spark_jobs.silver_cleaning")


@asset(deps=[silver_cleaned], group_name="silver")
def silver_located(context: AssetExecutionContext) -> None:
    run_module(context, "src.spark_jobs.silver_geolocation")


@asset(deps=[silver_located], group_name="silver")
def silver_tracked(context: AssetExecutionContext) -> None:
    run_module(context, "src.spark_jobs.silver_tracking")


class StibTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "gold"


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=StibTranslator())
def stib_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()




@asset(deps=[AssetKey(t) for t in GOLD_TABLES], group_name="serving")
def gold_exports(context: AssetExecutionContext) -> None:
    run_module(context, "src.export_gold")

defs = Definitions(
    assets=[bronze_positions, silver_cleaned, silver_located, silver_tracked, stib_dbt_models, gold_exports, ],
    resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
)