import datetime
import urllib
from abc import ABC, abstractmethod
from typing import Any, Generator, Optional

from pydantic import BaseModel, Field

from common.utils.logging_odis import logger

from ...data_source_model import APIModel, DataSourceModel, DomainModel
from .data_handler import IDataHandler, OperationType, PageLog

# from common.utils.file_handler import FileHandler


class ExtractionResult(BaseModel):
    """the result of an extraction operation"""

    success: bool = Field(..., description="is the page successfully extracted")
    payload: Any = Field(..., description="the extracted data")
    is_last: bool = Field(..., description="is this the last page")

    count: Optional[int] = Field(
        0, description="number of records extracted from the page"
    )

    total_count: Optional[int] = Field(
        0,
        description="Total number of records to extract, if specified in the response",
    )

    # TODO:
    # - improve typing here
    next_url: Optional[str] = Field(
        None,
        description="the URL of the next page",
        examples=[
            "https://api.insee.fr/melodi/data/DS_RP_LOGEMENT_PRINC?maxResult=10000&TIME_PERIOD=2021&RP_MEASURE=DWELLINGS&L_STAY=_T&TOH=_T&CARS=_T&NOR=_T&TSH=_T&BUILD_END=_T&OCS=_T&TDW=_T&page=2",
        ],
    )

    next_page: Optional[int] = Field(
        None, description="next page number, for use with pagenumber-style pagination"
    )

    next_token: Optional[str] = Field(
        None, description="next page token, for use with token-style pagination"
    )

    next_offset: Optional[int] = Field(
        None, description="next page offset, for use with offset-based pagination"
    )


class AbstractSourceExtractor(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'download' method is mandatory, which is responsible
    for pulling the data and storing it in a local file.
    """

    config: DataSourceModel
    url: str  # TODO: add type hint (HTTPUrl)
    handler: IDataHandler
    model: DomainModel
    api_config: APIModel

    def __init__(
        self, config: DataSourceModel, model: DomainModel, handler: IDataHandler = None
    ):
        """
        - populates the extractor with the configuration, the model and the handler
        - computes the full URL for the API endpoint

        Args:
            config (DataSourceModel): the configuration of the data source
            model (DomainModel): the model of the data source
            handler (FileHandler): the handler used to store the data
        """

        self.config = config
        self.model = model
        self.handler = handler

        # Decompose base API URl
        api = self.config.get_api(model)
        base_url = str(api.base_url) if api is not None else ""
        base_split = urllib.parse.urlsplit(base_url)

        # expand the URL endpoint path with the source config
        if base_split.path == "/":
            full_path = model.endpoint
        else:
            full_path = f"{base_split.path}{model.endpoint}"

        # rebuild the full URL with complete path
        self.url = urllib.parse.urljoin(f"https://{base_split.netloc}", full_path)

        self.api_config = self.config.get_api(model)

    @abstractmethod
    def download(self) -> Generator[ExtractionResult, None, None]:
        """Method to be implemented by the child class to download data from the API.
        The method should yield a ExtractionResult object for each page of data downloaded.
        """
        pass

    def execute(self) -> None:
        """Method to be called to start the extraction process.
        This method will download the data and store it in a local file; it will also
        store the metadata of the extraction in a separate file.

        Example:
        >>> extractor = MyExtractor(config, model, handler)
        >>> extractor.execute()
        """

        logger.debug(f"Processing data from {self.url}")

        start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        last_page_downloaded = 0
        page_logs = []
        errors = 0
        complete = False

        for result in self.download():

            last_page_downloaded += 1

            storage_info = self.handler.file_dump(self.model, data=result.payload)

            page_log_info = {
                "page": last_page_downloaded,
                "storage_info": storage_info,
                "success": result.success,
                "is_last": result.is_last,
            }

            if not result.success:
                errors += 1

            page_log = PageLog(**page_log_info)
            page_logs.append(page_log)

        # if the loop completes, extraction is successful
        complete = True

        # dump the execution metadata
        self.handler.dump_metadata(
            self.model,
            OperationType.EXTRACT,
            start_time=start_time,
            last_processed_page=last_page_downloaded,
            complete=complete,
            errors=errors,
            pages=page_logs,
        )
