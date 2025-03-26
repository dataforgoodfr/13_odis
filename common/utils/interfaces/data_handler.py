from typing import Any, Protocol

from pandas import DataFrame
from pydantic import BaseModel, NonNegativeInt
from typing import Optional

from common.data_source_model import DomainModel


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


class MetadataInfo(BaseModel):
    """Information about the metadata"""

    domain: str
    source: str
    operation: str
    last_run_time: str
    last_processed_page: NonNegativeInt
    complete: bool
    errors: int
    model: DomainModel
    pages: list[PageLog]


class IDataHandler(Protocol):
    """base interface to define a handler in charge of handling extracted data.

    A handler may be a file writer, a database writer, a data processor, etc.

    TODO:
        - improve interfacing to allow for different file formats (csv, json, etc)
        - better handling of metadata files
    """

    def file_dump(
        self, model: DomainModel, *args, data: Any, **kwargs
    ) -> StorageInfo: ...

    def json_load(self, page_log: PageLog, *args, **kwargs) -> dict: ...

    def csv_load(self, page_log: PageLog, *args, **kwargs) -> DataFrame: ...
