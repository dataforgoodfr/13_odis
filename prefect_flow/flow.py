# prefect_flow/flow.py

from prefect import flow, task
from pipeline.extract_service import run_extraction
from pipeline.load_service import run_load
from prefect_flow.generate_sources import generate_dbt_sources
from common.config import load_config
from common.data_source_model import DataSourceModel
import asyncio
import subprocess


@task
async def prefect_extract(config, ds, max_concurrency):
    # On appelle directement l'extraction async
    await run_extraction(config, [ds], max_concurrency)


@task
def prefect_load(config, ds):
    run_load(config, [ds])


@task
def prefect_dbt_run():
    subprocess.run(["dbt", "run"], check=True, capture_output=True, text=True)


@flow
async def full_pipeline(config_path: str, max_concurrency: int = 4):
    config = load_config(config_path, response_model=DataSourceModel)

    # Extraction en parallèle
    extract_tasks = [
        prefect_extract.submit(config, ds, max_concurrency)
        for ds in config.get_models().values()
    ]
    await asyncio.gather(*extract_tasks)

    generate_dbt_sources(config_path)

    # Load
    load_tasks = [
        prefect_load.submit(config, ds)
        for ds in config.get_models().values()
    ]

    # dbt run
    prefect_dbt_run.submit()

if __name__ == "__main__":
    full_pipeline.serve(
        name="full_pipeline",
        parameters={
            "config_path": "datasources.yaml",
            "max_concurrency": 4,
        },
        tags=["local"],
    ).apply()


    ####################### SANS UI
    #import asyncio

    #asyncio.run(
    #    full_pipeline(
    #        config_path="datasources.yaml",
    #        max_concurrency=4,
    #    )
    #)