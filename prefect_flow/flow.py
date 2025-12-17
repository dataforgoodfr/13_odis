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
from common.data_source_model import DataSourceModel, DomainModel
from common.utils.file_handler import DEFAULT_BASE_PATH, FileHandler
from common.utils.interfaces.data_handler import MetadataInfo
from common.utils.interfaces.data_handler import OperationType
import re

def artifact_key_safe(name: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")

def read_extract_metadata(ds: DomainModel) -> MetadataInfo | None:
    handler = FileHandler()

    try:
        return handler.load_metadata(
            model=ds,
            operation=OperationType.EXTRACT,
        )
    except Exception as e:
        # le fichier n'existe pas ou est invalide
        return None

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
    tasks = [
        prefect_extract.with_options(name=f"Extract {ds.name}").submit(
            config,
            ds,
            max_concurrency
        )
        for ds in config.get_models().values()
    ]

    logger_prefect.info("CHECK ARTIFACT")
    # On récupère les artifacts
    artifacts = [
        t.result(raise_on_failure=False)
        for t in tasks
    ]

    cleaned_artifacts = []

    for ds, future, result in zip(
        config.get_models().values(),
        tasks,
        artifacts
    ):
        if future.state.is_failed():
            await create_markdown_artifact(
                key=f"extract-{artifact_key_safe(ds.name)}",
                markdown=(
                    f"### Extraction `{ds.name}`\n"
                    f"❌ Erreur pendant l'extraction\n"
                    f"```\n{result}\n```"
                ),
                description=f"Extraction error for {ds.name}",
            )
        else:
            metadata = read_extract_metadata(ds)

            metadata_path = (
                f"{DEFAULT_BASE_PATH}/{ds.domain_name}/"
                f"{artifact_key_safe(ds.name)}_metadata_extract.json"
            )

            if metadata is None:
                await create_markdown_artifact(
                    key=f"extract-{artifact_key_safe(ds.name)}",
                    markdown=(
                        f"### Extraction `{ds.name}`\n"
                        f"❌ Aucun fichier metadata trouvé"
                    ),
                    description=f"Extraction failed for {ds.name}",
                )
            else :
                cleaned_artifacts.append({
                    "ds": ds,
                    "extract_ok": True,
                })

                status = "✅" if metadata.complete else "❌"
                await create_markdown_artifact(
                    key=f"extract-{artifact_key_safe(ds.name)}",
                    markdown=(
                        f"### Extraction `{ds.name}`\n"
                        f"{status} Extraction terminée\n\n"
                        f"📄 Metadata : `{metadata_path}`\n"
                        f"- Pages traitées : {metadata.last_processed_page}\n"
                        f"- Erreurs : {metadata.errors}"
                    ),
                    description=f"Extraction success for {ds.name}",
                )


    # PAS BESOIN DE PREFECT POUR TESTER CA
    #generate_dbt_sources(config_path)

    # Load tasks avec artifact_path
    load_futures = []

    for item in cleaned_artifacts:
        ds = item["ds"]

        if not item["extract_ok"]:
            continue

        metadata = read_extract_metadata(ds)

        if metadata is None or not metadata.complete:
            logger_prefect.warning(
                f"Skipping load for {ds.name}: extraction incomplete"
            )
            continue

        load_futures.append(
            prefect_load.with_options(name=f"Load {ds.name}").submit(
                config,
                ds,
            )
        )

    load_results = [
        f.result(raise_on_failure=False)
        for f in load_futures
    ]

    for future, result in zip(load_futures, load_results):
        if future.state.is_failed():
            logger_prefect.error(f"Load failed: {result}")

    dbt_future = prefect_dbt_run.with_options(name="DBT Run").submit()
    dbt_result = dbt_future.result(raise_on_failure=False)

    if dbt_future.state.is_failed():
        logger_prefect.error(f"dbt run failed: {dbt_result}")
