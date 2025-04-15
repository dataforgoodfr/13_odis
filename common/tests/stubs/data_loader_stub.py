from typing import Generator

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import IDataHandler, PageLog
from common.utils.interfaces.db_client import IDBClient
from common.utils.interfaces.loader import AbstractDataLoader, Column, ColumnType


class CommentedDataLoader(AbstractDataLoader):

    is_data_loaded = False
    col_name: str
    col_description: str

    def __init__(
        self,
        config: DataSourceModel,
        model: DomainModel,
        db_client: IDBClient,
        handler: IDataHandler = None,
        col_name: str = "data",
        col_description: str = "Data loaded as JSONB",
    ):

        self.config = config
        self.model = model
        self.db_client = db_client
        self.handler = handler
        self.col_name = col_name
        self.col_description = col_description

    def list_columns(self) -> list[Column]:
        """for json data, we only need a single column to store the jsonb data"""

        return [
            Column(
                name=self.col_name,
                data_type=ColumnType.JSON,
                description=self.col_description,
            )
        ]

    def load_data(self, pages: list[PageLog]) -> Generator[PageLog, None, None]:
        """Method to load pages from json files, indexed in a DataProcessLog object.
        Yields an iterable result with page number (int) and load success information (bool) for each page
        """

        for extract_page_log in pages:

            yield extract_page_log

        self.is_data_loaded = True
