from typing import AsyncGenerator

from common.utils.interfaces.extractor import AbstractSourceExtractor, ExtractionResult

from .data_handler import StubDataHandler


class StubExtractor(AbstractSourceExtractor):
    """used to test the JsonExtractor class"""

    is_download: bool = False

    def __init__(self, config, model, client, handler):
        super().__init__(config, model, client, handler)

    async def download(self) -> AsyncGenerator[ExtractionResult, None]:

        self.is_download = True
        yield ExtractionResult(payload={"test": "test"}, is_last=True, success=True)


class StubIterationExtractor(AbstractSourceExtractor):
    """a stub extractor that iterates 10 times"""

    is_download: bool = False
    iteration_count: int = 0
    expected_iterations: int = 10

    def __init__(self, config, model, client, handler: StubDataHandler):
        super().__init__(config, model, client, handler)

    async def download(self) -> AsyncGenerator[ExtractionResult, None]:

        self.is_download = True

        for _i in range(1, self.expected_iterations):
            self.iteration_count += 1
            yield ExtractionResult(payload={"test": "test"}, is_last=True, success=True)
