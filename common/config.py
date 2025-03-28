from typing import Type

import yaml


def load_config[T](file_path: str, response_model: Type[T] = dict) -> T:
    """
    the representation of the yaml is validated against the `response_model` provided,
    and the yaml is loaded into a python object of type `response_model`

    Args:
        file_path (str): the path to the yaml file
        response_model (Any): the model to validate the yaml against. Defaults to dict.

    Returns:
        Any: the python object representation of the yaml file

    Example:
    ```python
    from common.config import load_config
    from common.data_source_model import DataSourceModel

    config = load_config("path/to/config.yml", response_model=DataSourceModel)
    ```

    """

    with open(file_path, "r") as file:

        yaml_config = yaml.safe_load(file)

        return response_model(**yaml_config)
