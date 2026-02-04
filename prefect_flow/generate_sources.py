# src/prefect_flow/generate_sources.py
from pathlib import Path
from common.data_source_model import DataSourceModel
from common.config import load_config
import yaml

GENERATED_PATH = Path("prefect_flow/data/bronze__generated_sources.yml")

def generate_dbt_sources(config_path: str):
    config = load_config(config_path, response_model=DataSourceModel)
    models = config.get_models()

    # Structure dbt
    bronze_source = {
        "version": 2,
        "sources": [
            {
                "name": "bronze",
                "schema": "bronze",
                "tables": []
            }
        ]
    }

    for model in models.values():
        table_name = model.table_name
        file_path = f"prefect_flow/data/imports/{model.domain_name}/{model.name}.json"

        bronze_source["sources"][0]["tables"].append({
            "name": table_name,
            "description": f"Source JSON importée automatiquement pour {model.domain_name}/{model.name}",
            "meta": {
                 "path": file_path
            }
        })

    GENERATED_PATH.parent.mkdir(parents=True, exist_ok=True)
    GENERATED_PATH.write_text(yaml.dump(bronze_source, sort_keys=False))
    print(f"✔️ dbt sources generated: {GENERATED_PATH}")
