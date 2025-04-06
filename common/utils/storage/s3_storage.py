from datetime import datetime
from typing import Any

from pandas import DataFrame
from ...data_source_model import DomainModel
from ..interfaces.data_handler import IDataHandler, MetadataInfo, OperationType, PageLog, StorageInfo


class S3StorageHandler(IDataHandler):
    """Handler to manage S3 storage operations about extracted data"""

    def file_dump(
        self,
        model: DomainModel,
        *args,
        data: Any,
        suffix: str = None,
        format: str = None,
        **kwargs,
    ) -> StorageInfo:
        """Dumps the data into a file on S3 storage."""
        raise NotImplementedError("TBD")

    def json_load(self, page_log: PageLog, *args, suffix: str = None, **kwargs) -> dict:
        """Loads the JSON data from S3 storage."""
        raise NotImplementedError("TBD")

    def csv_load(self, page_log: PageLog, *args, **kwargs) -> DataFrame:
        """Loads the CSV data from S3 storage."""
        raise NotImplementedError("TBD")

    def load_metadata(self, model: DomainModel, operation: OperationType, *args, **kwargs) -> MetadataInfo:
        """Loads the metadata file from S3 storage."""
        raise NotImplementedError("TBD")

    def dump_metadata(
        self,
        model: DomainModel,
        operation: OperationType,
        *args,
        start_time: datetime = None,
        last_processed_page: int = 1,
        complete: bool = False,
        errors: int = 0,
        pages: list[PageLog] = None,
        **kwargs,
    ) -> MetadataInfo:
        """Dumps the metadata information into a file on S3 storage."""
        raise NotImplementedError("TBD")

        """Dumps the information about an operation run into a MetadataInfo object and into a file.

        Args:
            model (DomainModel): the model to be processed
            operation (OperationType): the type of operation to be performed
            start_time (datetime): the time when the operation started, if not provided, current time is used
            last_processed_page (int): the last page processed, default is 1
            complete (bool): True if the operation completed successfully, default is False
            errors (int): the number of errors encountered, default is 0
            pages (list[PageLog]): the list of page logs, default is None

        Returns:
            MetadataInfo: the metadata information about the operation
        """
        ...
