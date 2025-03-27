import datetime
import urllib
from abc import ABC, abstractmethod
from typing import Any, Generator, Optional

from pydantic import BaseModel, Field

from common.utils.logging_odis import logger

from ...data_source_model import APIModel, DataSourceModel, DomainModel
from .data_handler import IDataHandler, MetadataInfo, PageLog


class ExtractionResult(BaseModel):
    """the result of an extraction operation"""

    success: bool = Field(..., description="is the page successfully extracted")
    payload: Any = Field(..., description="the extracted data")
    is_last: bool = Field(..., description="is this the last page")

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
        None,
        description="next page number, for use with pagenumber-style pagination"
    )

    next_token: Optional[str] = Field(
        None,
        description="next page token, for use with token-style pagination"
    )

    next_offset: Optional[int] = Field(
        None,
        description="next page offset, for use with offset-based pagination"
    )


class AbstractSourceExtractor(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'download' method is mandatory, which is responsible
    for pulling the data and storing it in a local file.
    """

    config: DataSourceModel
    url: str  # TODO: add type hint (HTTPUrl)
    handler: IDataHandler = None
    metadata_handler: IDataHandler = None
    model: DomainModel
    api_config: APIModel

    def __init__(
        self,
        config: DataSourceModel,
        model: DomainModel,
        handler: IDataHandler,
        metadata_handler: IDataHandler,
    ):
        """
        - populates the extractor with the configuration, the model and the handler
        - computes the full URL for the API endpoint

        Args:
            config (DataSourceModel): the configuration of the data source
            model (DomainModel): the model of the data source
            handler (IDataHandler): the handler used to store the data
            metadata_handler (IDataHandler): the handler used to store the metadata
        """

        self.config = config
        self.handler = handler
        self.metadata_handler = metadata_handler
        self.model = model

        # Decompose base API URl
        base_url = str(self.config.get_api(model).base_url)
        base_split = urllib.parse.urlsplit(base_url)

        # expand the URL endpoint path with the source config
        if base_split.path == "/":
            full_path = model.endpoint
        else:
            full_path = f"{base_split.path}{model.endpoint}"

        # rebuild the full URL with complete path
        self.url = urllib.parse.urljoin(f"https://{base_split.netloc}", full_path)

        self.api_config = self.config.get_api(model)

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
                "is_last": result.is_last
            }

            if not result.success:
                errors += 1

            page_log = PageLog( **page_log_info )
            page_logs.append(page_log)
        
        # if the loop completes, extraction is successful
        complete = True

        # Export metadata info
        # just go through pydantic to ensure the data is valid
        # and process eventual inner things
        extract_metadata = MetadataInfo(
            **{
                "domain": self.config.get_domain_name(self.model),
                "source": self.model.name,
                "operation": 'extract',
                "last_run_time": start_time.isoformat(),
                "last_processed_page": last_page_downloaded,
                "complete": complete,
                "errors": errors,
                "model": self.model,
                "pages": page_logs
            }
        ).model_dump( 
            mode = "json" 
        ) 

        meta_info = self.metadata_handler.file_dump(
            self.model,
            data = extract_metadata
            )

        logger.debug(
            f"Metadata written in: '{meta_info.location}/{meta_info.file_name}'"
        )

    @abstractmethod
    def download(self, *args, **kwargs) -> Generator[ExtractionResult, None, None]:
        """Method to be implemented by the concrete extractor class.
        It should return a generator that yields ExtractionResult objects.

        Example:
        ```python

        def download(self):
            for page in range(1, 10):
                data = self.get_page(page)
                is_last = page == 9
                yield ExtractionResult(payload=data, is_last=is_last)
        ```
        """
        pass
