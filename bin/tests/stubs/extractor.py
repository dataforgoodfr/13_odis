from typing import AsyncGenerator

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import IDataHandler
from common.utils.interfaces.extractor import AbstractSourceExtractor, ExtractionResult
from common.utils.interfaces.http import HttpClient


class StubExtractor(AbstractSourceExtractor):

    return_value: ExtractionResult
    is_called: bool = True

    def __init__(
        self,
        return_value: ExtractionResult,
        config: DataSourceModel,
        model: DomainModel,
        http_client: HttpClient,
        handler: IDataHandler,
    ):
        self.return_value = return_value
        super().__init__(
            config=config,
            model=model,
            http_client=http_client,
            handler=handler,
        )

    async def download(self) -> AsyncGenerator[ExtractionResult, None]:
        # Simulate a download operation
        print("Mocking download of data")
        self.is_called = True
        yield self.return_value
