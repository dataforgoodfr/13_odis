from typing import Any, Protocol

import datetime

from pandas import DataFrame
from pydantic import BaseModel, NonNegativeInt
from typing import Optional, Literal

from common.data_source_model import DomainModel
from common.utils.logging_odis import logger


OPERATION_TYPE = Literal["extract", "load", "harvest"]    

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
    operation: OPERATION_TYPE
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


    def dump_metadata(self,
                      model: DomainModel,
                      operation: OPERATION_TYPE, 
                      start_time: datetime = None,
                      last_processed_page: int = None,
                      complete: bool = None,
                      errors: int = None,
                      pages: list[PageLog] = None
                      ) -> MetadataInfo:
        """Dumps the information about an operation run into a MetadataInfo object and into a file.
        Arguments :
            - model : DomainModel
            - operation: OPERATION_TYPE  # supported values : 'extract', 'load', 'harvest' 
            - start_time: datetime = None
            - last_processed_page: int = None
            - complete: bool = None
            - errors: int = None # count of failed iterations
            - pages: list[PageLog] = None

        Returns a MetadataInfo object
        """

        # Export metadata info
        # just go through pydantic to ensure the data is valid
        # and process eventual inner things
        operation_metadata = MetadataInfo(
            **{
                "domain": model.domain_name,
                "source": model.name,
                "operation": operation,
                "last_run_time": start_time.isoformat(),
                "last_processed_page": last_processed_page,
                "complete": complete,
                "errors": errors,
                "model": model,
                "pages": pages
            }
        )
        
        meta_payload = operation_metadata.model_dump( 
            mode = "json" 
        ) 

        meta_dump_info = self.file_dump(
            model,
            data = meta_payload,
            suffix = f"metadata_{operation}",
            format = "json"
            )

        logger.debug(
            f"Metadata written in: '{meta_dump_info.location}/{meta_dump_info.file_name}'"
        )

        return operation_metadata

