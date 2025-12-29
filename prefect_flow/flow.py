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
from pathlib import Path
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt_odis"
PROFILES_DIR = DBT_DIR

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

#@task
#def prefect_dbt_run():
#    runner = PrefectDbtRunner(
#        settings=PrefectDbtSettings(
#            project_dir=str(DBT_DIR),
#            profiles_dir=str(PROFILES_DIR)
#        )
#    )
#    runner.invoke(["build"])  # ou ["run"] selon ce que tu veux faire


def get_dbt_runner():
    return PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=str(DBT_DIR),
            profiles_dir=str(PROFILES_DIR),
        )
    )


@task(
    name="dbt build model",
    retries=1,
    retry_delay_seconds=10,
)
def dbt_build_model(model_name: str):
    runner = get_dbt_runner()

    runner.invoke([
        "build",
        "--select",
        f"{model_name}",
    ])


def run_dbt_models_from_load_tasks(load_tasks_by_model: dict[str, any]):
    dbt_tasks = []

    for model_name, load_task in load_tasks_by_model.items():
        dbt_task = dbt_build_model.with_options(
            name=f"dbt build {model_name}"
        ).submit(
            model_name,
            wait_for=[load_task],  # 🔥 lien explicite
        )

        dbt_tasks.append(dbt_task)

    for t in dbt_tasks:
        t.result(raise_on_failure=False)

@task(name="Extract")
async def prefect_extract(config, ds, max_concurrency):
    logger = get_run_logger()

    try:
        await run_extraction(config, [ds], max_concurrency)

        metadata = read_extract_metadata(ds)
        if metadata is None:
            raise RuntimeError("Metadata extract not found")

        status = "✅" if metadata.complete else "❌"

        metadata_file = next(
            (
                a.storage_info.file_name
                for a in (metadata.artifacts or [])
                if "metadata_extract" in a.name
            ),
            f"{ds.name}_metadata_extract.json",
        )

        metadata_path = (
            f"{DEFAULT_BASE_PATH}/{ds.domain_name}/{metadata_file}"
        )

        await create_markdown_artifact(
            key=f"extract-{artifact_key_safe(ds.name)}",
            markdown=(
                f"### Extraction `{ds.name}`\n"
                f"{status} Extraction terminée\n\n"
                f"📄 Metadata : `{metadata_path}`\n"
                f"- Pages traitées : {metadata.last_processed_page}\n"
                f"- Erreurs : {metadata.errors}"
            ),
            description=f"Extraction {ds.name}",
        )

        if not metadata.complete:
            raise RuntimeError(
                f"Extraction incomplete ({metadata.errors} errors)"
            )

        return metadata

    except Exception as e:
        await create_markdown_artifact(
            key=f"extract-{artifact_key_safe(ds.name)}",
            markdown=(
                f"### Extraction `{ds.name}`\n"
                f"❌ Échec\n\n"
                f"```\n{e}\n```"
            ),
            description=f"Extraction failed for {ds.name}",
        )
        raise


@task(name="Load", retries=0)
def prefect_load(config, ds):
    logger = get_run_logger()

    try:
        run_load(config, [ds])

        create_markdown_artifact(
            key=f"load-{artifact_key_safe(ds.name)}",
            markdown=f"### Load `{ds.name}`\n✅ Load terminé",
            description=f"Load success for {ds.name}",
        )

    except Exception as e:
        create_markdown_artifact(
            key=f"load-{artifact_key_safe(ds.name)}",
            markdown=(
                f"### Load `{ds.name}`\n"
                f"❌ Échec\n\n"
                f"```\n{e}\n```"
            ),
            description=f"Load failed for {ds.name}",
        )
        raise


@flow
async def full_pipeline(config_path: str = "datasources.yaml", max_concurrency: int = 4):
    config = load_config(config_path, response_model=DataSourceModel)

    # PAS BESOIN DE PREFECT POUR TESTER CA
    #generate_dbt_sources(config_path)

    extract_tasks = [
        prefect_extract.with_options(
            name=f"Extract {ds.name}"
        ).submit(config, ds, max_concurrency)
        for ds in config.get_models().values()
    ]

    extract_results = [
        t.result(raise_on_failure=False)
        for t in extract_tasks
    ]

    valid_metadata = []

    for task, result in zip(extract_tasks, extract_results):
        if task.state.is_failed():
            continue
        if not isinstance(result, MetadataInfo):
            continue
        if not result.complete:
            continue

        valid_metadata.append(result)

    load_tasks = []
    load_tasks_by_model = {}

    for metadata in valid_metadata:
        ds = metadata.model  # si MetadataInfo contient le DomainModel ou DataSourceModel
        load_task = prefect_load.with_options(name=f"Load {ds.name}").submit(config, ds)
        load_tasks.append(load_task)
        load_tasks_by_model[ds.name] = load_task



    for t in load_tasks:
        t.result(raise_on_failure=False)  # ← propagation naturelle

    run_dbt_models_from_load_tasks(load_tasks_by_model)