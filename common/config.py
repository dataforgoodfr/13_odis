import yaml
from pydantic_core import ValidationError

from .data_source_model import DataSourceModel


def load_config(file_path: str, strict: bool = True) -> dict:
    """
    the dict representation of the yaml is validated

    Args:
        file_path (str): the path to the yaml file
        strict (bool): if True, the yaml file must match a `DataSourceModel` schema, defaults to True

    """

    with open(file_path, "r") as file:

        try:

            yaml_config = yaml.safe_load(file)

            if strict:
                return DataSourceModel(**yaml_config).model_dump(mode="json")

            return yaml_config

        except (yaml.YAMLError, TypeError, ValidationError):
            raise
