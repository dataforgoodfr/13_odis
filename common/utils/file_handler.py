import json
from bson import ObjectId
from pathlib import Path

from common.utils.logging_odis import logger

DEFAULT_BASE_PATH = 'data/imports'
DEFAULT_FILE_FORMAT = 'json'

class bJSONEncoder(json.JSONEncoder):
    """Utility class to encode JSON or bJSON data, for JSON file I/O"""

    def default(self,o):
        if isinstance(o,ObjectId):
            return str(o)
        json.JSONEncoder.default(self,o)

class FileHandler():

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

        # Write payload content to file
        if format == 'json':

            with open(filepath,"w") as f:
                # encode json data
                encoder = bJSONEncoder()
                encoded_json = encoder.encode(payload)
                loaded_json = json.loads(encoded_json)
                json.dump(loaded_json,f)
            
        else: 
            with open(filepath,"wb") as f:
                f.write(payload)

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
        


