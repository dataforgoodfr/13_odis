import json
from pathlib import Path
from typing import Any

import orjson
from bson import ObjectId

from common.data_source_model import DomainModel
from common.utils.logging_odis import logger

from .interfaces.data_handler import IDataHandler, StorageInfo

DEFAULT_BASE_PATH = "data/imports"
DEFAULT_FILE_FORMAT = "json"


class bJSONEncoder(json.JSONEncoder):
    """Utility class to encode JSON or bJSON data, for JSON file I/O"""

    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        json.JSONEncoder.default(self, o)


class FileHandler(IDataHandler):
    """
    a handler to save data to a file
    """

    base_path: str
    _file_name: str
    _index: int = 0

    def __init__(
        self,
        base_path: str = DEFAULT_BASE_PATH,
        file_name: str = None,
    ):
        """
        Args:
            base_path (str, optional): where to store the files, Defaults to 'data/imports'.
            file_name (str, optional): the name of the file.
                Defaults to None, in which case the file name is generated
        """

        self.base_path = base_path
        self._file_name = file_name

    def _data_dir(self, model: DomainModel) -> Path:
        """Generate the directory Path where the data will be stored"""
        return Path(f"{self.base_path}/{model.API}")

    def file_name(self, model: DomainModel) -> str:
        """Generate the file name for the given model and page number

        If a file name was provided at initialization, it will be used instead of the generated one

        When a file name is generated, the name pattern is the following:
        `{model.name}_{current_index}.{model.format}` where `current_index` is an internal counter that is incremented each time a file name is generated

        Args:
            model (DomainModel): the model that generated the data
        """

        if self._file_name:
            return self._file_name

        # increment the index to avoid overwriting
        self._index += 1

        return f"{model.name}_{self._index}.{model.format}"

    def file_dump(self, model: DomainModel, data: Any) -> StorageInfo:
        """
        saves the data to a file and returns the storage info

        Args:
            model (DomainModel): the model that generated the data
            data (Any): the data to save

        Returns:
            StorageInfo: the storage info, including the location of the file
        """

        # Create data directory if it doesn't exist
        data_dir = self._data_dir(model)
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        filepath = data_dir / self.file_name(model)

        # Write payload content to file
        # case where we store a metadata file, the data is a dict although the model may not be json
        if isinstance(data, dict) or model.format == "json":

            with open(filepath, "w") as f:
                try:
                    if isinstance(data, bytes):
                        data = data.decode()
                    f.write(orjson.dumps(data).decode())

                except Exception as e:
                    logger.error(f"Error encoding JSON data: {str(e)}")

        else:
            with open(filepath, "wb") as f:
                f.write(data)

        logger.info(f"{model.name} -> results saved to : '{filepath}'")

        return StorageInfo(
            location=str(data_dir),
            format=model.format,
            file_name=filepath.name,
            encoding="utf-8",
        )

    def json_load(
        self,
        storage_info: StorageInfo,
    ) -> dict:
        """Opinionated utility method to load JSON files.

        Args :
            storage_info (StorageInfo) : the storage info of the file to load
            base_path (str) : the base path where the file is stored

        Return decoded JSON data into a python dict"""

        payload = {}

        filepath = storage_info.location / storage_info.file_name
        logger.debug(f"File path: {filepath}")

        try:
            logger.debug(f"loading JSON file : {filepath}")
            with open(filepath, "r") as f:
                payload = orjson.loads(f.read())

        except json.JSONDecodeError as e:
            logger.exception(f"Invalid JSON format in {filepath}: {str(e)}")

        except Exception as e:
            logger.exception(f"Error reading file {filepath}: {str(e)}")

        return payload
