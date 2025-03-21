from typing import Generator

from common.utils.interfaces.extractor import ExtractionResult
from common.utils.source_extractors import JsonExtractor


class StubExtractor(JsonExtractor):
    """used to test the JsonExtractor class"""

    is_download: bool = False

    def download(self) -> Generator[ExtractionResult]:

        self.is_download = True
        yield ExtractionResult(
            payload={"test": "test"}, page_number=1, is_last=True, filepath="test"
        )
