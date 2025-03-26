import time
import urllib
from typing import Generator

import jmespath
import requests

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.extractor import AbstractSourceExtractor, ExtractionResult
from common.utils.logging_odis import logger

from .file_handler import FileHandler


class FileExtractor(AbstractSourceExtractor):
    """Generic extractor for a file dump from an API"""

    is_json: bool = False
    handler: FileHandler  # typing
    metadata_handler: FileHandler  # typing

    def __init__(self, config: DataSourceModel, model: DomainModel):
        super().__init__(
            config,
            model,
            handler=FileHandler(),
            metadata_handler=FileHandler(file_name=f"{model.name}_extract_log.json"),
        )

    def download(self) -> Generator[ExtractionResult, None, None]:
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """

        # Send request to API
        response = requests.get(
            self.url,
            headers=self.model.headers.model_dump(mode="json"),
            params=self.model.extract_params,
        )
        response.raise_for_status()
        payload = response.json() if self.is_json else response.content

        # yield the request result
        yield ExtractionResult(payload=payload, is_last=True)


class JsonExtractor(FileExtractor):
    """Extractor for getting JSON data from an API"""

    is_json = True


class MelodiExtractor(FileExtractor):
    """Extractor for getting JSON data from an API"""

    def download(self) -> Generator[ExtractionResult, None, None]:

        is_last = False
        url = self.url

        while not is_last:

            # yield the request result
            result = self.download_page(url)

            is_last = result.is_last

            if not is_last and result.next_url:

                time.sleep(60 / self.api_config.throttle)

                url = result.next_url

                logger.debug(f"Next page: {result.next_url}")

            yield result

    def download_page(self, url: str) -> ExtractionResult:
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """

        # if url has a query string, ignore the dict-defined parameters
        url_querystr = urllib.parse.urlparse(url).query
        passed_params = self.model.extract_params if url_querystr == "" else None
        logger.info(f"querying '{url}'")

        # Send request to API
        response = requests.get(
            url,
            headers=self.model.headers.model_dump(mode="json"),
            params=passed_params,
        )
        response.raise_for_status()

        payload = response.json()

        # Get next page URL
        next_key = self.model.response_map.get("next")
        next_url = jmespath.search(next_key, payload) if next_key else None

        # Determine whether this is the last page :
        # if explicitly given by the API response, get the explicit value
        # if not given in API response, BUT if no next page could be derived, then true
        # else: false
        is_last_key = self.model.response_map.get("is_last")
        is_last = (
            jmespath.search(is_last_key, payload) if is_last_key else (next_url is None)
        )

        return ExtractionResult(
            payload=payload,
            is_last=is_last,
            next_url=next_url,
        )
