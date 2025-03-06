from abc import ABC, abstractmethod
import requests
import urllib
import datetime
import json
from bson import ObjectId
from pathlib import Path

DEFAULT_FILE_FORMAT = 'json'

class bJSONEncoder(json.JSONEncoder):
    """Utility class to encode JSON or bJSON data, for JSON file I/O"""

    def default(self,o):
        if isinstance(o,ObjectId):
            return str(o)
        json.JSONEncoder.default(self,o)

class SourceExtractor(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'download' method is mandatory, which is responsible
    for pulling the data and storing it in a local file.
    """
    api_confs : dict
    source_models : dict
    url : str
    headers : dict
    params : dict
    format : str

    @abstractmethod
    def download(self, domain: str, source_config):
        pass

    def set_query_parameters(self, source_model_name: str):
        """Set all parameteres for an API call from the source model configuration, given in datasources.yaml :

            - builds and sets the complete URL to be queried
            - sets the specified headers
            - sets the query parameters, if specified in the source model's yaml block
            - sets the expected output file format

            Headers default to the API definition's 'default_headers', if any.
            
            Return : None. Only sets the above as properties of the current Extractor object.
        """

        # Get the dict definitions of the source model and its related API
        source_model = self.source_models[source_model_name]
        api_name = source_model['API']
        api_conf = self.api_confs[api_name]

        # Decompose base API URl
        base_url = api_conf['base_url']
        base_split = urllib.parse.urlsplit(base_url)

        # expand the query path with the source config
        full_path = f"{base_split.path}{source_model['endpoint']}"
        
        # rebuild the full URL with complete path
        self.url = urllib.parse.urljoin(f"https://{base_split.netloc}", full_path)
        
        # Add logging entry once logging PR is merged
        # logger.debug(f"URL: {self.url}")
        
        # Set headers with the order of preference :
        # Specified in source model > specified in API defaults > None
        default_headers = api_conf.get('default_headers')
        self.headers = source_model.get('headers', default_headers)
        
        # Set request parameters if any
        self.params = source_model.get('params')
        
        # Set expected output file format
        self.format = source_model.get('format', DEFAULT_FILE_FORMAT)


class FileExtractor(SourceExtractor):
    """Generic extractor for one-shot file downloads from an API"""

    def __init__(self,config: dict, domain: str):

        self.api_confs = config['APIs']
        self.source_models = config['domains'][domain]

    def download(self, domain: str, source_model_name: str):
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """
        
        start_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

        # Set up the request : url, headers, query parameters
        self.set_query_parameters(source_model_name)

        # Send request to API
        response = requests.get(self.url, headers=self.headers, params=self.params)
        response.raise_for_status()

        # Create data directory if it doesn't exist
        data_dir = Path(f"data/imports/{domain}")
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        filename = f"{start_time}_{domain}_{source_model_name}.{self.format}"
        filepath = data_dir / filename

        # Write response content to file
        with open(filepath, 'wb') as f:
            f.write(response.content)

        return str(filepath)

class JsonExtractor(SourceExtractor):
    """Extractor for getting JSON data from an API"""

    def __init__(self,config: dict, domain: str):

        self.api_confs = config['APIs']
        self.source_models = config['domains'][domain]

    def download(self, domain: str, source_model_name: str):
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """
        
        start_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

        # Set up the request : url, headers, query parameters
        self.set_query_parameters(source_model_name)

        # Send request to API
        response = requests.get(self.url, headers=self.headers, params=self.params)
        response.raise_for_status()

        raw_result = response.json()

        # Create data directory if it doesn't exist
        data_dir = Path(f"data/imports/{domain}")
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        filename = f"{start_time}_{domain}_{source_model_name}.json"
        filepath = data_dir / filename

        # encode json data
        encoder = bJSONEncoder()
        encoded_json = encoder.encode(raw_result)
        loaded_json = json.loads(encoded_json)
        
        # Write response json content to file
        with open(filepath,"w") as f:
            json.dump(loaded_json,f)

        return str(filepath)
