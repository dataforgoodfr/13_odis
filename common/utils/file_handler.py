import json
from pathlib import Path
from typing import Any

import orjson
import pandas as pd
from bson import ObjectId

from common.data_source_model import FILE_FORMAT, DomainModel
from common.utils.exceptions import InvalidCSV, InvalidJson
from common.utils.logging_odis import logger

from .interfaces.data_handler import IDataHandler, PageLog, StorageInfo

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

    TODO:
    - better interfacing to allow for different file formats (csv, json, etc)
    - better handling of metadata files
    """

    base_path: str
    _index: int = 0

    def __init__(
        self,
        base_path: str = DEFAULT_BASE_PATH
    ):
        """
        Args:
            base_path (str, optional): where to store the files, Defaults to 'data/imports'.
        """

        self.base_path = base_path

    def _data_dir(self, model: DomainModel) -> Path:
        """Generate the directory Path where the data will be stored"""
        return Path(f"{self.base_path}/{model.domain_name}")

    def file_name(
        self, model: DomainModel, suffix: str = None, format: FILE_FORMAT = None
    ) -> str:
        """Generate the file name for the given model and suffix

        When a file name is generated, the name pattern is the following:
        - if a suffix is provided, it is appended to the model name
        - otherwise, an index is appended to the model name

        Args:
            model (DomainModel): the model that generated the data
            suffix (str, optional): a suffix to append to the file name. Defaults
                to None, in which case an index is appended to the model name
            format (str, optional): expected file format. Defaults to the model's file format
        """

        # If format not specified, apply the Model's file format
        if format is None:
            format = model.format

        name = ""
        # increment the index to avoid overwriting
        self._index += 1

        if suffix:
            name = f"{model.name}_{suffix}.{format}"

        else:
            name = f"{model.name}_{self._index}.{format}"

        return name

    def file_dump(
        self,
        model: DomainModel,
        data: Any,
        suffix: str = None,
        format: FILE_FORMAT = None,
    ) -> StorageInfo:
        """
        saves the data to a file and returns the storage info

        Args:
            model (DomainModel): the model that generated the data
            data (Any): the data to save
            suffix (str, optional): a suffix to append to the file name. Defaults
                to None.

        Returns:
            StorageInfo: the storage info, including the location of the file
        """
        # If format not specified, apply the Model's file format
        if format is None:
            format = model.format

        # Create data directory if it doesn't exist
        data_dir = self._data_dir(model)
        data_dir.mkdir(parents=True, exist_ok=True)

        file_name = self.file_name(
            model, suffix=suffix, format=format
        )  # suffix is optional
        # Generate filename from source name
        filepath = data_dir / file_name

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
        page_log: PageLog,
    ) -> dict:
        """Parses a JSON file and returns the decoded data

        Args :
            page_log (PageLog) : the info where the file is stored

        Return decoded JSON data into a python dict

        Raises:
            InvalidJson: if the file is not found or the JSON is invalid

        """

        filepath = Path(page_log.storage_info.location) / Path(
            page_log.storage_info.file_name
        )

        try:
            logger.debug(f"loading JSON file : {filepath}")
            with open(filepath, "r") as f:
                return orjson.loads(f.read())

        except json.JSONDecodeError as e:
            logger.exception(f"Invalid JSON format in {filepath}: {str(e)}")

        except Exception as e:
            logger.exception(f"Error reading file {filepath}: {str(e)}")

        raise InvalidJson(f"Error reading file '{filepath}'")

    def csv_load(
        self,
        page_log: PageLog,
        model: DomainModel,
    ) -> pd.DataFrame:
        """Parses a CSV file and returns the data as an iterator of `dict`

        TODO:
        - benchmark usage of pandas vs csv module

        Args:
            page_log (PageLog) : the info where the file is stored
            model (DomainModel): the model that generated the data

        Returns:
            DataFrame: the data from the CSV file as a pandas DataFrame

        Raises:
            InvalidCSV: if the file is not found or the CSV is invalid
        """

        filepath = Path(page_log.storage_info.location) / Path(
            page_log.storage_info.file_name
        )

        try:
            logger.debug(f"loading CSV file : {filepath}")
            return pd.read_csv(
                filepath,
                header=model.load_params.header,
                skipfooter=model.load_params.skipfooter,
                sep=model.load_params.separator,
                engine="python",  # Required for skipfooter parameter
            )

        except Exception as e:
            logger.exception(f"Error reading file {filepath}: {str(e)}")

        raise InvalidCSV(f"Error reading file '{filepath}'")
