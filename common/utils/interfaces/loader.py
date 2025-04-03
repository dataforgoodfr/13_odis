import datetime
from abc import ABC, abstractmethod
from typing import Generator

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import IDataHandler, OperationType, PageLog
from common.utils.interfaces.db_client import IDBClient


class AbstractDataLoader(ABC):
    """Interface for data loaders."""

    config: DataSourceModel
    model: DomainModel
    handler: IDataHandler
    db_client: IDBClient

    def __init__(
        self,
        config: DataSourceModel,
        model: DomainModel,
        db_client: IDBClient,
        handler: IDataHandler = None,
    ):

        self.config = config
        self.model = model
        self.db_client = db_client
        self.handler = handler

    @abstractmethod
    def load_data(self) -> Generator[PageLog, None, None]:
        pass

    @abstractmethod
    def create_or_overwrite_table(self):
        pass

    def execute(self, overwrite_table: bool = True):
        """
        loads extracted data into the target system

        Raises:
            RuntimeError: if the metadata of extracted sources cannot be loaded

        """

        if overwrite_table:
            # create bronze table drop if it already exists
            self.create_or_overwrite_table()

        start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        last_processed_page = 0
        page_logs = []
        errors = 0
        complete = False

        # load the latest extract metadata
        try:
            extract_metadata = self.handler.load_metadata(
                self.model, operation=OperationType.EXTRACT
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to load 'extract' metadata for model '{self.model.name}': {str(e)}"
            ) from e

        # execute loader iterations and log results in new metadata
        result: PageLog  # typing hint for IDE
        for result in self.load_data(extract_metadata.pages):

            last_processed_page += 1

            if not result.success:
                errors += 1

            page_logs.append(result)

        # if the loop completes, extraction is successful
        complete = True

        self.handler.dump_metadata(
            self.model,
            OperationType.LOAD,
            start_time=start_time,
            last_processed_page=last_processed_page,
            complete=complete,
            errors=errors,
            pages=page_logs,
        )
