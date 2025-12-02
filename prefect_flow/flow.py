# prefect_flow/flow.py
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pipeline.extract_service import run_extraction
from pipeline.load_service import run_load
from prefect_flow.generate_sources import generate_dbt_sources
from common.config import load_config
from common.data_source_model import DataSourceModel
import asyncio
import subprocess
from prefect.logging import get_run_logger
import asyncio
import time
from typing import List
from common.data_source_model import DataSourceModel, DomainModel
from common.utils.factory.extractor_factory import create_extractor
from common.utils.file_handler import FileHandler
from common.utils.http.async_client import AsyncHttpClient
from common.utils.logging_odis import logger


@task(log_prints=True)
async def prefect_extract(config, ds, max_concurrent_requests):
    """
    Extraction pour une datasource + artifact Prefect 3.6
    """
    logger_prefect  = get_run_logger()
    logger_prefect.info("INFO level log message from a task.")

    start_time = time.time()
    http_client = AsyncHttpClient(max_connections=max_concurrent_requests)
    logger_prefect  = get_run_logger()

    logger_prefect.info("ON EST LA")
    print("ON EST LA")

    tasks = []
    for ds in [ds]:
        logger.info(f"[extract] preparing extractor for {ds.name}")
        logger_prefect.info(f"[extract] preparing extractor for {ds.name}")

        extractor = create_extractor(
            config, ds, http_client=http_client, handler=FileHandler()
        )
        tasks.append(extractor.execute())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger_prefect = get_run_logger()
    # Detect errors
    errors = [res for res in results if isinstance(res, Exception)]
    if errors:
        for err in errors:
            logger.error(f"[extract] error: {err}")
            logger_prefect.error(f"[extract] error: {err}")
            print(f"[extract] error: {err}")

        raise RuntimeError(f"{len(errors)} extraction errors occurred")

    elapsed = time.time() - start_time
    logger.info(f"[extract] completed in {elapsed:.2f}s for {len([ds])} sources")
    logger_prefect.info(f"[extract] completed in {elapsed:.2f}s for {len([ds])} sources")
    print(f"[extract] completed in {elapsed:.2f}s for {len([ds])} sources")

    artifact_path = results[0] if results else None

    if artifact_path:
        create_markdown_artifact(
            key=f"extract-{ds.name}",
            markdown=(
                f"### Extraction pour `{ds.name}`\n"
                f"- Fichier généré : `{artifact_path}`"
            ),
            description=f"Resultat extraction {ds.name}"
        )

    return {"ds_name": ds.name, "artifact_path": artifact_path}


@task(log_prints=True)
async def prefect_load(config, ds, artifact_path):
    """
    Load SQL + artifact Prefect
    """
    if artifact_path is None:
        return f"Skipped load for {ds.name}"

    await run_load(config, [ds])

    create_markdown_artifact(
        key=f"load-{ds.name}",
        markdown=(
            f"### Load pour `{ds.name}`\n"
            f"- Fichier utilisé : `{artifact_path}`\n"
            f"- Table SQL créée : `{ds.table_name}`"
        ),
        description=f"Load SQL pour {ds.name}"
    )

    return f"Loaded {ds.name}"


@task(log_prints=True)
def prefect_dbt_run():
    subprocess.run(["dbt", "run"], check=True, capture_output=True, text=True)


@flow
async def full_pipeline(config_path: str, max_concurrency: int = 4):
    logger_prefect = get_run_logger()
    logger_prefect.info("START FLOW")
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


if __name__ == "__main__":
    full_pipeline.serve(
        name="full_pipeline",
        parameters={"config_path": "datasources.yaml", "max_concurrency": 4},
        tags=["local"],
    ).apply()
