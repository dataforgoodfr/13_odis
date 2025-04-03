import time
import urllib
from typing import Generator

import jmespath
import nbformat
import requests
from nbconvert.preprocessors import ExecutePreprocessor

from common.utils.interfaces.extractor import AbstractSourceExtractor, ExtractionResult
from common.utils.logging_odis import logger


class FileExtractor(AbstractSourceExtractor):
    """Generic extractor for a file dump from an API"""

    is_json: bool = False

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
                jmespath.search(is_last_key, payload)
                if is_last_key
                else (next_url is None)
            )

            # If all went well, success = true
            success = True

        except Exception as e:
            error = str(e)
            logger.exception(f"Error while extracting page {url}: {error}")

        return ExtractionResult(
            success=success, payload=payload, is_last=is_last, next_url=next_url
        )


class NotebookExtractor(FileExtractor):
    """Extractor for getting JSON data from an API"""

    def download(self) -> Generator[ExtractionResult, None, None]:
        """
        execute the notebook and yield the result

        Yields:
            ExtractionResult: the result of the notebook execution
                each cell output yielded as a separate ExtractionResult
                the last cell output is marked as is_last=True
                the next_url is None

        Example:
            >>> extractor = NotebookExtractor(config, model)
            >>> for result in extractor.download():
            ...     print(result.payload)
        """

        logger.info(f"Executing notebook '{self.model.notebook_path}'")

        with open(self.model.notebook_path) as f:
            nb_in = nbformat.read(f, nbformat.NO_CONVERT)

            ep = ExecutePreprocessor(timeout=600)
            ep.preprocess(
                nb_in,
            )

            payload = []

            for cell in nb_in.cells:
                if cell.cell_type == "code" and "outputs" in cell:
                    for out in cell.outputs:
                        if out.output_type == "execute_result":
                            payload.append(out.data.get("text/plain"))

            yield ExtractionResult(
                payload=payload,
                is_last=True,
                success=True,
            )

            # last cell output


class OpenDataSoftExtractor(FileExtractor):

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
            response = requests.get(
                url,
                headers=self.model.headers.model_dump(mode="json"),
                params=passed_params,
            )
            response.raise_for_status()

            raw_data = response.json()

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
