import urllib
import time
from typing import Generator
import jmespath

# from common.data_source_model import FILE_FORMAT
from common.utils.interfaces.extractor import SynchronousExtractor, ExtractionResult
from common.utils.interfaces.data_handler import DataArtifact
from common.utils.logging_odis import logger


class MelodiExtractor(SynchronousExtractor):
    """Extractor for getting JSON data from an API"""

    def download(self) -> Generator[DataArtifact, None, None]:

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

    def download_page(self, url: str) -> DataArtifact:
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """

        # if url has a query string, ignore the dict-defined parameters
        url_querystr = urllib.parse.urlparse(url).query
        passed_params = self.model.extract_params if url_querystr == "" else None
        logger.info(f"querying '{url}' with params: {passed_params}")

        success = False
        payload = None
        next_url = None
        is_last = False

        try:
            # Send request to API
            response = self.http_client.get(
                url,
                headers=self.model.headers.model_dump(mode="json"),
                params=passed_params,
                as_json=True,
            )

            # logger.debug(payload)

            # Get actual payload
            next_key = self.model.response_map.get("data")
            payload = jmespath.search(next_key, response) if next_key else response

            # Get next page URL
            next_key = self.model.response_map.get("next")
            next_url = jmespath.search(next_key, response) if next_key else None

            # Determine whether this is the last page :
            # if explicitly given by the API response, get the explicit value
            # if not given in API response, BUT if no next page could be derived, then true
            # else: false
            is_last_key = self.model.response_map.get("is_last")
            logger.debug(f'is_last_key: {is_last_key}')
            logger.debug(f'found the value for {is_last_key}: {jmespath.search(is_last_key, response)}')

            is_last = (
                jmespath.search(is_last_key, response)
                if is_last_key
                else (next_url is None)
            )

            # If all went well, success = true
            success = True

            logger.debug(f'is_last: {is_last}')

            return DataArtifact(
                name=self.model.name,
                success=success, 
                payload=payload, 
                next_url=next_url,
                is_last=is_last
            )

        except Exception as e:
            error = str(e)
            logger.exception(f"Error while extracting page {url}: {error}")

            return DataArtifact(
                name=self.model.name,
                success=False, 
                payload=[], 
                next_url=None,
                is_last=True
            )



class OpenDataSoftExtractor(SynchronousExtractor):

    def download(self) -> Generator[ExtractionResult, None, None]:

        is_last = False
        url = self.url

        pageno = 1
        offset = 0
        total_count = 0
        aggregated_count = 0

        while not is_last:

            # yield the request result
            result = self.download_page(url, offset)

            is_last = result.is_last
            total_count = result.total_count
            page_records_count = result.count
            # Accumulate number of records
            aggregated_count += page_records_count

            logger.debug(f"Extracted {page_records_count} from page {pageno}")
            logger.debug(f"Extracted {aggregated_count} out of {total_count} so far.")

            time.sleep(60 / self.api_config.throttle)

            offset = aggregated_count + 1

            logger.debug(f"Next offset: {offset}")

            yield result

    def download_page(self, url, offset) -> ExtractionResult:

        passed_params = self.model.extract_params
        passed_params["offset"] = offset

        logger.info(f"querying '{url}'")

        success = False
        payload = None
        count = 0
        total_count = 0

        try:
            # Send request to API

            raw_data = self.http_client.get(
                url,
                headers=self.model.headers.model_dump(mode="json"),
                params=passed_params,
                as_json=True,
            )

            # get payload
            datapath = self.model.response_map.get("data")
            payload = jmespath.search(datapath, raw_data)

            # get records count
            count = len(payload)

            # get total count
            total_count_key = "total_count"
            total_count = jmespath.search(total_count_key, raw_data)

            # If offset + count goes is greater thatn total_count,
            # means this is the last call to be made
            is_last = offset + count > total_count

            # If all went well, success = true
            success = True

        except Exception as e:
            error = str(e)
            logger.exception(f"Error while extracting page {url}: {error}")

        return ExtractionResult(
            success=success,
            payload=payload,
            is_last=is_last,
            count=count,
            total_count=total_count,
        )
