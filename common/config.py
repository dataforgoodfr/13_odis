import yaml
from pydantic_core import ValidationError

from .data_source_model import DataSourceModel


def load_config(file_path: str) -> dict:
    """the dict representation of the yaml is validated"""

    with open(file_path, "r") as file:

        try:

            yaml_config = yaml.safe_load(file)

            return DataSourceModel(**yaml_config).model_dump()

        except (yaml.YAMLError, TypeError, ValidationError):
            raise
