# src/prefect_flow/flow.py

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
    subprocess.run(["dbt", "run"], check=True)


@flow
async def full_pipeline(config_path: str, max_concurrency: int = 4):
    config = load_config(config_path, response_model=DataSourceModel)

    # 1. Extraction : on lance toutes les tasks Prefect en parallèle
    extract_tasks = [
        prefect_extract.submit(config, ds, max_concurrency)
        for ds in config.get_models().values()
    ]

    # On doit attendre les tasks via leur future Prefect
    await asyncio.gather(*(t.wait_for_completion() for t in extract_tasks))

    # 2. Génération sources dbt
    generate_dbt_sources(config_path)

    # 3. Load dans PostgreSQL
    for ds in config.get_models().values():
        prefect_load.submit(config, ds)

    # 4. dbt run
    prefect_dbt_run.submit()
