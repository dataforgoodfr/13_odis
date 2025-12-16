# prefect_flow/flow.py

import sys
sys.path.append('../')

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pipeline.extract_service import run_extraction
from pipeline.load_service import run_load
from prefect_flow.generate_sources import generate_dbt_sources
from common.config import load_config
import subprocess
from prefect.logging import get_run_logger
import asyncio
from common.data_source_model import DataSourceModel


@task
async def prefect_extract(config, ds, max_concurrency):
    await run_extraction(config, [ds], max_concurrency)

@task
def prefect_load(config, ds):
    run_load(config, [ds])

@task(log_prints=True)
def prefect_dbt_run():
    subprocess.run(["dbt", "run"], check=True, capture_output=True, text=True)


@flow
async def full_pipeline(config_path: str = "datasources.yaml", max_concurrency: int = 4):
    logger_prefect = get_run_logger()
    logger_prefect.info("START FLOW")
    logger_prefect.info("START DATA EXTRACT")
    config = load_config(config_path, response_model=DataSourceModel)

    # Extraction tasks dynamiques
    extract_tasks = [
        prefect_extract.with_options(name=f"Extract {ds.name}").submit(
            config,
            ds,
            max_concurrency
        )
        for ds in config.get_models().values()
    ]

    logger_prefect.info("CHECK ARTIFACT")
    # On récupère les artifacts
    artifacts = await asyncio.gather(*extract_tasks, return_exceptions=True)

    cleaned_artifacts = []

    for ds, result in zip(config.get_models().values(), artifacts):
        if isinstance(result, Exception):
            # On crée un faux artifact expliquant l’erreur
            cleaned_artifacts.append({
                "ds_name": ds.name,
                "artifact_path": None,
                "error": str(result),
            })

            # On crée un artifact Prefect indiquant l'échec
            create_markdown_artifact(
                key=f"extract-{ds.name}",
                markdown=f"### Extraction `{ds.name}`\n❌ Erreur pendant l'extraction\n```\n{result}\n```",
                description=f"Extraction error for {ds.name}",
            )
        else:
            cleaned_artifacts.append(result)


    generate_dbt_sources(config_path)

    # Load tasks avec artifact_path
    load_futures = [
        prefect_load.with_options(name=f"Extract {ds.name}").submit(
            config,
            ds,
            artifact["artifact_path"],
        )
        for ds, artifact in zip(config.get_models().values(), cleaned_artifacts)
    ]
    await asyncio.gather(*[f.wait() for f in load_futures])

    dbt_future = prefect_dbt_run.with_options(name="DBT Run").submit()
    await dbt_future.wait()
