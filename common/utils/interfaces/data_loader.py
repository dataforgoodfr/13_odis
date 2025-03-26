import datetime
from abc import ABC, abstractmethod
from typing import Generator

import orjson
from pydantic import ValidationError

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.database_client import DatabaseClient
from common.utils.file_handler import FileHandler
from common.utils.interfaces.data_handler import MetadataInfo, PageLog
from common.utils.logging_odis import logger


class AbstractDataLoader(ABC):
    """Interface for data loaders."""

    config: DataSourceModel
    model: DomainModel
    handler: FileHandler
    metadata_handler: FileHandler

    def __init__(self, config: DataSourceModel, model: DomainModel):
        self.config = config
        self.model = model

        self.handler = FileHandler()
        self.metadata_handler = FileHandler(
            file_name=f"{model.name}_metadata_extract.json"
        )

    def execute(self, overwrite_table: bool = True):

        if overwrite_table:
            # create bronze table drop if it already exists
            self.create_or_overwrite_table()

        start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        last_processed_page = 0
        page_logs = []
        errors = 0
        complete = False

        # load actual data from metadata
        metadata = self.load_metadata()

        # execute loader iterations and log results in metadata
        result: PageLog  # typing hint for IDE
        for result in self.load_data(metadata.pages):

            last_processed_page += 1

            if not result.success:
                errors += 1

            page_logs.append(result)

        # if the loop completes, extraction is successful
        complete = True

        # Export metadata info
        # just go through pydantic to ensure the data is valid
        # and process eventual inner things
        load_metadata = MetadataInfo(
            **{
                "domain": self.config.get_domain_name(self.model),
                "source": self.model.name,
                "operation": "load_data",
                "last_run_time": start_time.isoformat(),
                "last_processed_page": last_processed_page,
                "complete": complete,
                "errors": errors,
                "model": self.model,
                "pages": page_logs,
            }
        ).model_dump(mode="json")

        # Dump the Load metadata into a new file
        meta_info = self.handler.file_dump(
            self.model, data=load_metadata, suffix="metadata_load"
        )

        logger.debug(
            f"Metadata written in: '{meta_info.location}/{meta_info.file_name}'"
        )

    @abstractmethod
    def load_data(self) -> Generator[PageLog, None, None]:
        pass

    def load_metadata(self) -> MetadataInfo:
        """
        TODO: metadata could be stored in a different location, or in a DB
        """

        metadata_filepath = self.metadata_handler._data_dir(
            self.model
        ) / self.metadata_handler.file_name(self.model)

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

    def create_or_overwrite_table(self):
        """Creates the target Bronze table.
        If exists, drops table to recreate"""

        # domain_name = self.config.get_domain_name(self.model)

        # initiate database session
        db = DatabaseClient(autocommit=False)

        # create bronze table drop if it already exists
        # table_name = f"{domain_name}_{self.model.table_name}"

        logger.info(f"Creating table bronze.{self.model.table_name}")

        db.execute(f"DROP TABLE IF EXISTS bronze.{self.model.table_name}")
        db.execute(
            f"""
            CREATE TABLE bronze.{self.model.table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        """
        )
        db.commit()
