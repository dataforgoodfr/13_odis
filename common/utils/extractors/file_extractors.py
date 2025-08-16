from typing import AsyncGenerator
from common.utils.interfaces.extractor import AsynchronousExtractor, ExtractionResult


class FileExtractor(AsynchronousExtractor):
    """Generic extractor for a file dump from an API"""

    is_json: bool = False

    async def download(self) -> AsyncGenerator[ExtractionResult, None]:
        """Downloads data corresponding to the given source model.
        The parameters of the request (URL, headers etc) are set using the inherited set_query_parameters method.
        """

        # Send request to API
        response = await self.http_client.get(
            self.url,
            headers=self.model.headers.model_dump(mode="json"),
            params=self.model.extract_params,
            as_json=self.is_json,
        )

        # yield the request result
        yield ExtractionResult(payload=response, success=True, is_last=True)


class JsonExtractor(FileExtractor):
    """Extractor for getting JSON data from an API"""

    is_json = True
