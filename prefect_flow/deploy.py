# deploy.py
from pathlib import Path
from flow import full_pipeline

def main() -> None:
    """Deploy the POS ETL flow to the 'etl' work pool."""
    project_root = Path(__file__).resolve().parents[1]

    full_pipeline.from_source(
        source=project_root,
        entrypoint="prefect_flow/flow.py:full_pipeline",
    ).deploy(  # type: ignore
        name="full-pipeline",
        # C'est le nom de mon work pool Prefect en local, ca peut etre n'importe quoi
        # prefect work-pool create etl-process --type process pour creer le work pool
        # prefect worker start --pool etl-process va directement demarrer un worker pour ce work pool 
        work_pool_name="default",
        tags=["to-delete"],
    )


if __name__ == "__main__":
    main()