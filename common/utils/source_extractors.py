import time
import urllib
from typing import Generator

import jmespath
import requests

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.extractor import AbstractSourceExtractor, ExtractionResult
from common.utils.logging_odis import logger

from .file_handler import FileHandler

import os
import boto3
import datetime


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
            metadata_handler=FileHandler(file_name=f"{model.name}_metadata_extract.json"),
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
        yield ExtractionResult(payload=payload, success=True, is_last=True)


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

        success = False
        payload = None
        is_last = False
        next_url = None

        try:
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
            
            # If all went well, success = true
            success = True
        
        except Exception as e:
            error = str(e)
            logger.exception(f"Error while extracting page {url}: {error}")

        return ExtractionResult(
            success=success,
            payload=payload,
            is_last=is_last,
            next_url=next_url
        )

class XlsxExtractDumpS3(AbstractSourceExtractor):
    """Extractor for XLSX files that:
    - Downloads from a given URL (defined in YAML)
    - Dumps the raw XLSX to S3 (e.g., Scaleway)
    - Uses domain.subdomain from the model name to construct S3 key
    """

    def __init__(self, config, model, handler=None, metadata_handler=None):
        super().__init__(config, model, handler, metadata_handler)
        
        # Logging for debug
        logger.debug(f"Initializing XlsxExtractDumpS3 with model: {model}")

        # Extract domain/subdomain
        domain, subdomain = self.model.name.split(".")

        # S3 setup
        self.s3_bucket = os.getenv("SCW_BUCKET_NAME")
        if not self.s3_bucket:
            raise ValueError("Missing SCW_BUCKET_NAME environment variable")
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        self.s3_key = f"raw/{domain}/{subdomain}_{timestamp}.xlsx"

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("SCW_OBJECT_STORAGE_ENDPOINT"),
            aws_access_key_id=os.getenv("SCW_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SCW_SECRET_KEY"),
            region_name=os.getenv("SCW_REGION"),
        )

    def download(self) -> Generator[ExtractionResult, None, None]:
        """Download the XLSX file and upload it directly to S3."""
        try:
            logger.debug(f"Downloading XLSX file from {self.url}")
            response = requests.get(self.url)
            response.raise_for_status()

            # Upload to S3 (SCW)
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.s3_key,
                Body=response.content,
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            logger.debug(f"File uploaded to s3://{self.s3_bucket}/{self.s3_key}")

            # Still yield so execute() can proceed
            yield ExtractionResult(payload=response.content, is_last=True, success=True)

        except Exception as e:
            logger.error(f"Failed to download/upload XLSX file: {e}")
            raise
    
    # Overriding get_api to bypass any API lookup
    def get_api(self, model: DomainModel):
        return None