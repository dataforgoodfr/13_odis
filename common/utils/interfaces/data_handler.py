from typing import Any, Protocol

from pandas import DataFrame
from pydantic import BaseModel, NonNegativeInt

from common.data_source_model import DomainModel


class StorageInfo(BaseModel):
    """Information about the storage location"""

    location: str
    format: str
    file_name: str
    encoding: str


class FileDumpInfo(BaseModel):
    """Information about the file dump"""

    page: NonNegativeInt
    storage_info: StorageInfo


class MetadataInfo(BaseModel):
    """Information about the metadata"""

    domain: str
    source: str
    last_run_time: str
    last_page_downloaded: NonNegativeInt
    successfully_completed: bool
    file_dumps: list[FileDumpInfo]


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

    def json_load(self, filedump: FileDumpInfo, *args, **kwargs) -> dict: ...

    def csv_load(self, filedump: FileDumpInfo, *args, **kwargs) -> DataFrame: ...
