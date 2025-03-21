import json
from pathlib import Path
from typing import Any

import orjson
from bson import ObjectId

from common.data_source_model import DomainModel
from common.utils.logging_odis import logger

from .interfaces.data_handler import IDataHandler, StorageInfo

DEFAULT_BASE_PATH = "data/imports"


class bJSONEncoder(json.JSONEncoder):
    """Utility class to encode JSON or bJSON data, for JSON file I/O"""

    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        json.JSONEncoder.default(self, o)


<<<<<<< HEAD
    def file_dump(self, domain: str, source_name: str, payload = None, format: str = DEFAULT_FILE_FORMAT, page_number: int = None, base_path:str = DEFAULT_BASE_PATH) -> str:
        """Opinionated utility method to dump a payload files (json by default).
        Args :
                - domain: the Domain Name
                - source_name : the datasource name
                - payload : data to be dumped. Can be dict (for json format) or any (for binary file writing)
                - page_number : the page number if needed
                - format : file extension to use. Defaults to 'json'
                - base_path : folder where is the file to look for. Defaults to 'data/imports' 
                
        Returns the resulting dumped file's path as a str"""

        # Create data directory if it doesn't exist
        data_dir = Path(f"{base_path}/{domain}")
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        suffix = str(page_number) if page_number else 'full'
        filename = f"{domain}_{source_name}_{suffix}.{format}"
        filepath = data_dir / filename
=======
class FileHandler(IDataHandler):
    """
    TODO:
    - test this class (init, ..)
    """

    base_path: str
    _file_name: str
    _index: int = 0

    def __init__(
        self,
        base_path: str = DEFAULT_BASE_PATH,
        file_name: str = None,
    ):
        """_summary_

        Args:
            base_path (str, optional): _description_. Defaults to DEFAULT_BASE_PATH.
            file_format (str, optional): _description_. Defaults to DEFAULT_FILE_FORMAT.
            file_name (str, optional): _description_. Defaults to None.
        """

        self.base_path = base_path
        self._file_name = file_name

    def _data_dir(self, model: DomainModel) -> Path:
        """Generate the directory Path where the data will be stored"""
        return Path(f"{self.base_path}/{model.API}")

    def file_name(
        self,
        model: DomainModel,
    ) -> str:
        """Generate the file name for the given model and page number

        If a file name was provided at initialization, it will be used instead of the generated one

        When a file name is generated, the name pattern is the following:
        `{model.name}_{current_index}.{model.format}` where `current_index` is an internal counter that is incremented each time a file name is generated

        TODO: test
        """

        if self._file_name:
            return self._file_name

        # increment the index to avoid overwriting
        self._index += 1

        return f"{model.name}_{self._index}.{model.format}"

    def handle(self, model: DomainModel, data: Any) -> StorageInfo:
        """TODO: testing"""

        # Create data directory if it doesn't exist
        data_dir = self._data_dir(model)
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        filepath = data_dir / self.file_name(model)
>>>>>>> 42ef62f (refacto: tmp waiting for tests)

        # Write payload content to file
        # case where we store a metadata file, the data is a dict although the model may not be json
        if model.format == "json" or isinstance(data, dict):

            with open(filepath, "w") as f:
                try:
                    if isinstance(data, bytes):
                        data = data.decode()
                    f.write(orjson.dumps(data).decode())

<<<<<<< HEAD
        logger.info(f"Payload {domain} / {source_name} / page: {suffix} saved to : {filepath}")

        return str(filepath)

    def json_load(self, filepath:str = None, domain:str = None, source_name:str = None, page_number:int = None, format:str = DEFAULT_FILE_FORMAT, base_path:str = DEFAULT_BASE_PATH) -> dict:
        """Opinionated utility method to load JSON files.
            Args :
                - filepath:str -> if provided, just loads the filepath assuming it as json (and throws error if wrong format).
                If filepath not provided, the following args are used :
                - domain: the Domain Name
                - source_name : the datasource name
                - page_number : the page number if needed
                - format : file extension to look for. Defaults to 'json'
                - base_path : folder where is the file to look for. Defaults to 'data/imports 
                
            Either 'filepath' or 'domain' + 'source_name' must be provided.
            
            Return decoded JSON data into a python dict"""
        
        payload = {}

        # Raise error if necessary inputs were not provided
        if filepath is None and (domain is None or source_name is None):
            raise ValueError(f"{__name__}:: json_load : Not enough arguments provided ! Expected either 'filepath', or 'domain' + 'source_name'.")
        
        if filepath is None:

            data_dir = Path(f"{base_path}/{domain}")

            # Generate filename from source name
            suffix = str(page_number) if page_number else 'full'
            filename = f"{domain}_{source_name}_{suffix}.{format}"
            filepath = data_dir / filename
            logger.debug(f"File path: {filepath}")
        
        try:
            logger.debug(f"loading JSON file : {filepath}")
            with open(filepath, 'r') as f:
                payload = json.load(f)

        except json.JSONDecodeError as e:
            logger.exception(f"Invalid JSON format in {filepath}: {str(e)}")

        except Exception as e:
            logger.exception(f"Error reading file {filepath}: {str(e)}")

        return payload
        


=======
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
>>>>>>> 42ef62f (refacto: tmp waiting for tests)
