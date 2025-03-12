from abc import ABC, abstractmethod
import requests
import urllib
import jmespath
import time
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils.logging_odis import logger
from common.utils.file_handler import FileHandler

fh = FileHandler()

DEFAULT_FILE_FORMAT = "json"


class SourceExtractor(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'download' method is mandatory, which is responsible
    for pulling the data and storing it in a local file.
    """

    api_confs: dict
    source_models: dict
    url: str
    headers: dict
    params: dict
    format: str
    throttle: int = 0
    response_map: dict = {}

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
        api_name = source_model["API"]
        api_conf = self.api_confs[api_name]

        # Decompose base API URl
        base_url = api_conf["base_url"]
        base_split = urllib.parse.urlsplit(base_url)

        # expand the query path with the source config
        full_path = f"{base_split.path}{source_model['endpoint']}"

        # rebuild the full URL with complete path
        self.url = urllib.parse.urljoin(f"https://{base_split.netloc}", full_path)

        # Add logging entry once logging PR is merged
        # logger.debug(f"URL: {self.url}")
        
        # Set api throttle, defaults to 30 requests / minute
        self.throttle = api_conf.get('throttle', 30)
        
        # Set headers with the order of preference :
        # Specified in source model > specified in API defaults > None
        default_headers = api_conf.get("default_headers")
        self.headers = source_model.get("headers", default_headers)

        # Set request parameters if any
        self.params = source_model.get("params")

        # Set expected output file format
        self.format = source_model.get("format", DEFAULT_FILE_FORMAT)

        # Set Response mapping guide, empty dict if not provided
        self.response_map = source_model.get('response_map', {})


class FileExtractor(SourceExtractor):
    """Generic extractor for one-shot file downloads from an API"""

    def __init__(self, config: dict, domain: str):

        self.api_confs = config["APIs"]
        self.source_models = config["domains"][domain]

    def download(self, domain: str, source_model_name: str):
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """

        # Set up the request : url, headers, query parameters
        self.set_query_parameters(source_model_name)

        # Send request to API
        response = requests.get(self.url, headers=self.headers, params=self.params)
        response.raise_for_status()
        payload = response.content

        filepath = fh.file_dump(domain, source_model_name, payload = payload, format = self.format)

        page_number = 1
        is_last = True

        # yield the request result
        yield payload, page_number, is_last, filepath



class JsonExtractor(SourceExtractor):
    """Extractor for getting JSON data from an API"""

    def __init__(self, config: dict, domain: str):

        self.api_confs = config["APIs"]
        self.source_models = config["domains"][domain]

    def download(self, domain: str, source_model_name: str):
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """
        
        # Set up the request : url, headers, query parameters
        self.set_query_parameters(source_model_name)

        # Send request to API
        response = requests.get(self.url, headers=self.headers, params=self.params)
        response.raise_for_status()

        payload = response.json()

        filepath = fh.file_dump(domain, source_model_name, payload = payload, format = self.format)

        page_number = 1
        is_last = True

        # yield the request result
        yield payload, page_number, is_last, filepath


class MelodiExtractor(SourceExtractor):
    """Extractor for getting JSON data from an API"""

    def __init__(self, config: dict, domain: str):

        self.api_confs = config["APIs"]
        self.source_models = config["domains"][domain]

    def download(self, domain: str, source_model_name: str):
        # Set up the request : url, headers, query parameters
        self.set_query_parameters(source_model_name)
        
        is_last = False
        page_number = 0

        while not is_last:
            
            page_number += 1
            logger.info(f"querying {self.url}")
            logger.debug(f"query params: {self.params}")

            # Fetch next page's data
            payload, next, is_last, filepath = self.download_page(domain, source_model_name, page_number)

            # yield the request result
            yield payload, page_number, is_last, filepath
            
            # Update URL to be queried for next iteration
            self.url = next

            # Throttle before next iteration
            time.sleep(60 / self.throttle)

    def download_page(self, domain: str, source_model_name: str, page_number: int = 1):
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """
        
        # if needed, update self.params with params in the URL query string
        url_params = urllib.parse.parse_qsl(urllib.parse.urlparse(self.url).query)
        self.params |= url_params

        logger.debug(f"URL PARAMS: {url_params}")
        logger.debug(f"UPDATED PARAMS: {self.params}")

        # Send request to API
        response = requests.get(self.url, headers=self.headers, params=self.params)
        response.raise_for_status()

        payload = response.json()

        # Dump response file
        filepath = fh.file_dump(domain, source_model_name, payload = payload, page_number = page_number)

        # Get next page URL
        next_key = self.response_map.get('next')
        next = jmespath.search( next_key, payload ) if next_key else None

        # Determine whether this is the last page : 
        # if explicitly given by the API response, get the explicit value
        # if not given in API response, BUT if no next page could be derived, then true
        # else: false
        is_last = False
        is_last_key = self.response_map.get('is_last')
        is_last = jmespath.search( is_last_key, payload ) if is_last_key else (next is None)

        logger.debug(f"Next Page URL : {next}")
        logger.debug(f"Mapped key for is_last : {is_last_key}")
        logger.debug(f"Is this page the last one ? {is_last}")

        return payload, next, is_last, filepath
        
