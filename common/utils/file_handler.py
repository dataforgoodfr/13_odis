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

    base_path: str = DEFAULT_BASE_PATH

    def file_dump(self, domain: str, source_model_name: str, payload = None, format: str = DEFAULT_FILE_FORMAT, page_number: int = None):

        # Create data directory if it doesn't exist
        data_dir = Path(f"{self.base_path}/{domain}")
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        suffix = str(page_number) if page_number else 'full'
        filename = f"{domain}_{source_model_name}_{suffix}.{format}"
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

        logger.info(f"Payload {domain} / {source_model_name} / page: {suffix} saved to : {filepath}")

        return str(filepath)