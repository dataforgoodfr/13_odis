import datetime
from abc import ABC, abstractmethod
from typing import Generator

import orjson
from pydantic import ValidationError

from common.data_source_model import FILE_FORMAT, DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import (
    IDataHandler,
    MetadataInfo,
    OperationType,
    PageLog,
)
from common.utils.interfaces.db_client import IDBClient
from common.utils.logging_odis import logger


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
                f"Failed to load 'extract' metadata for model {self.model.name}: {str(e)}"
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

        self.dump_metadata(
            "load",
            start_time=start_time,
            last_processed_page=last_processed_page,
            complete=complete,
            errors=errors,
            pages=page_logs,
        )

    def dump_metadata(
        self,
        operation: str = "extract",
        start_time: datetime = None,
        last_processed_page: int = None,
        complete: bool = None,
        errors: int = None,
        pages: list[PageLog] = None,
    ):

        # Export metadata info
        # just go through pydantic to ensure the data is valid
        # and process eventual inner things
        extract_metadata = MetadataInfo(
            **{
                "domain": self.config.get_domain_name(self.model),
                "source": self.model.name,
                "operation": operation,
                "last_run_time": start_time.isoformat(),
                "last_processed_page": last_processed_page,
                "complete": complete,
                "errors": errors,
                "model": self.model,
                "pages": pages,
            }
        ).model_dump(mode="json")

        meta_info = self.handler.file_dump(
            self.model,
            data=extract_metadata,
            suffix=f"metadata_{operation}",
            format="json",
        )

        logger.debug(
            f"Metadata written in: '{meta_info.location}/{meta_info.file_name}'"
        )

    def load_metadata(self, metadata_type, format: FILE_FORMAT = None) -> MetadataInfo:
        self.handler.dump_metadata(
            self.model,
            operation=OperationType.LOAD,
            start_time=start_time,
            last_processed_page=last_processed_page,
            complete=complete,
            errors=errors,
            pages=page_logs,
        )

    def load_metadata(self, operation: OPERATION_TYPE) -> MetadataInfo:
        """
        TODO:
            - metadata could be stored in a different location, on an Object storage bucket, or in a DB
            - move this function to the DataHandler class
        """

        # If format not specified, apply the Model's file format
        if format is None:
            format = self.model.format

        metadata_filepath = self.handler._data_dir(self.model) / self.handler.file_name(
            self.model, suffix=f"metadata_{metadata_type}", format=format
        )

        metadata_filepath = self.handler._data_dir(self.model) / self.handler.file_name(
            self.model,
            suffix=f"metadata_{operation}",  # always the same pattern
            format="json",  # metadata are always json
        )
        try:

            with open(metadata_filepath, "r") as f:
                metadata = orjson.loads(f.read())

            return MetadataInfo(**metadata)

        except orjson.JSONDecodeError as e:
            logger.exception(f"Invalid JSON format in {metadata_filepath}: {str(e)}")

        except ValidationError as e:
            logger.exception(
                f"Invalid metadata format in {metadata_filepath}: {str(e)}"
            )

        except Exception as e:
            logger.exception(f"Error reading file {metadata_filepath}: {str(e)}")

        raise
