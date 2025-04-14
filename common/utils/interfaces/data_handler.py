import datetime
from enum import StrEnum
from typing import Any, Optional, Protocol

from pandas import DataFrame
from pydantic import BaseModel, NonNegativeInt

from common.data_source_model import DomainModel


class OperationType(StrEnum):
    """Enum for the operation type"""

    EXTRACT = "extract"
    LOAD = "load"
    PREPROCESS = "preprocess"
    HARVEST = "harvest"


class StorageInfo(BaseModel):
    """Information about the storage location"""

    location: str
    format: str
    file_name: str
    encoding: str


class PageLog(BaseModel):
    """model for easily updating and logging information about the processing of a given page"""

    page: NonNegativeInt
    storage_info: Optional[StorageInfo] = None
    is_last: Optional[bool] = False
    success: Optional[bool] = False

class ArtifactLog(BaseModel):
    """model for easily updating and logging information about the processing of an artifact file"""
    name: str
    storage_info: Optional[StorageInfo] = None
    load_to_bronze: Optional[bool] = False
    success: Optional[bool] = False

class MetadataInfo(BaseModel):
    """Information about the metadata"""

    domain: str
    source: str
    operation: OperationType
    last_run_time: str
    last_processed_page: NonNegativeInt
    complete: bool
    errors: int
    model: DomainModel
    pages: list[PageLog]
    artifacts: list[ArtifactLog]


class IDataHandler(Protocol):
    """base interface to define a handler in charge of handling extracted data
    and metadata.

    A handler may be a file writer, a database writer, a data processor, etc.

    """

    def file_dump(
        self,
        model: DomainModel,
        *args,
        data: Any,
        suffix: str = None,
        format: str = None,
        **kwargs,
    ) -> StorageInfo: ...

    def json_load(
        self, page_log: PageLog, *args, suffix: str = None, **kwargs
    ) -> dict: ...

    def csv_load(self, page_log: PageLog, *args, **kwargs) -> DataFrame: ...

    def load_metadata(
        self, model: DomainModel, operation: OperationType, *args, **kwargs
    ) -> MetadataInfo: ...

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
