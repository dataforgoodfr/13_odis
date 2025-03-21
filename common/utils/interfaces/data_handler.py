from typing import Any, Protocol

from pydantic import BaseModel

from common.data_source_model import DomainModel


class StorageInfo(BaseModel):
    """Information about the storage location"""

    location: str
    format: str
    file_name: str
    encoding: str


class IDataHandler(Protocol):
    """base interface to define a handler in charge of handling extracted data.

    A handler may be a file writer, a database writer, a data processor, etc.
    """

    def handle(self, model: DomainModel, *args, data: Any, **kwargs) -> StorageInfo: ...
