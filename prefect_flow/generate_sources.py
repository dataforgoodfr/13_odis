# src/prefect_flow/generate_sources.py
from pathlib import Path
from common.data_source_model import DataSourceModel
import yaml

GENERATED_PATH = Path("prefect_flow/data/bronze__generated_sources.yml")

def generate_dbt_sources(config_path: str):
    config = DataSourceModel.load_from_yaml(config_path)
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
        table_name = f"{model.domain}_{model.model}"
        file_path = f"prefect_flow/data/imports/{model.domain}/{model.model}.json"

        bronze_source["sources"][0]["tables"].append({
            "name": table_name,
            "description": f"Source JSON importée automatiquement pour {model.domain}/{model.model}",
            "meta": {
                 "path": file_path
            }
        })

    GENERATED_PATH.parent.mkdir(parents=True, exist_ok=True)
    GENERATED_PATH.write_text(yaml.dump(bronze_source, sort_keys=False))
    print(f"✔️ dbt sources generated: {GENERATED_PATH}")
