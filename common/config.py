import yaml
from pydantic_core import ErrorDetails, ValidationError

from .data_source_model import DataSourceModel


def _extract_err_info(e: ErrorDetails) -> str:
    if len(e["loc"]) > 0:
        return f"'{e['loc'][0]}' -> {e['msg']} , value passed: '{str(e['input'])}'"
    return e["msg"]


def load_config(file_path: str) -> dict:
    """the dict representation of the yaml is validated"""

    with open(file_path, "r") as file:

        try:

            yaml_config = yaml.safe_load(file)

            return DataSourceModel(**yaml_config).model_dump()

        except (yaml.YAMLError, TypeError) as e:
            raise Exception(f"Error parsing YAML file: {str(e)}")

        except ValidationError as v:
            raise Exception(
                f"""
                Error validating YAML file, the structure is not correct,
                please refer to the documentation for the correct structure. 
                Following errors were found: 
                {
                    ','.join(
                        map(
                            _extract_err_info,
                            v.errors(include_context=True, include_url=True),
                        )
                    ) 
                }
                """
            )
