from typing import Generator

from common.utils.interfaces.extractor import AbstractSourceExtractor, ExtractionResult


class StubExtractor(AbstractSourceExtractor):
    """used to test the JsonExtractor class"""

    is_download: bool = False

    def __init__(self, config, model, handler, metadata_handler):
        super().__init__(config, model, handler, metadata_handler)

    def download(self) -> Generator[ExtractionResult, None, None]:

        self.is_download = True
        yield ExtractionResult(
            payload={"test": "test"}, page_number=1, is_last=True, filepath="test"
        )


class StubIterationExtractor(AbstractSourceExtractor):
    """a stub extractor that iterates 10 times"""

    is_download: bool = False
    iteration_count: int = 0
    expected_iterations: int = 10

    def __init__(self, config, model, handler, metadata_handler):
        super().__init__(config, model, handler, metadata_handler)

    def download(self) -> Generator[ExtractionResult, None, None]:

        self.is_download = True

        for _i in range(1, self.expected_iterations):
            self.iteration_count += 1
            yield ExtractionResult(
                payload={"test": "test"}, page_number=1, is_last=True, filepath="test"
            )
