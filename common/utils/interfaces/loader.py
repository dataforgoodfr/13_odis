import datetime
import re
import unicodedata
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Generator, Optional

import psycopg2
from pydantic import BaseModel, ValidationInfo, field_validator

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import IDataHandler, OperationType, PageLog
from common.utils.interfaces.db_client import IDBClient
from common.utils.logging_odis import logger


class ColumnType(StrEnum):
    """Enum for column types."""

    TEXT = "TEXT"
    JSON = "JSONB"


class Column(BaseModel):
    """Column model."""

    name: str
    description: Optional[str] = None
    data_type: Optional[ColumnType] = None
    is_primary_key: Optional[bool] = False

    @field_validator("name", mode="after")
    @classmethod
    def sanitize_name(cls, value: str, info: ValidationInfo) -> str:
        """
        Sanitize column names by:
        1. Replacing spaces with underscores
        2. Removing accents
        3. Ensuring the name is SQL-friendly
        4. remove surrounding quotes
        5. Ensuring the name starts with a letter
        6. Converting to lowercase
        7. Removing any non-alphanumeric characters except underscores
        """
        # Normalize unicode characters and remove accents
        normalized = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
        # Replace spaces with underscores
        sanitized = normalized.replace(" ", "_")
        # Remove any non-alphanumeric characters except underscores
        sanitized = re.sub(r"[^\w]", "", sanitized)
        # Remove surrounding quotes
        sanitized = sanitized.strip('"').strip("'")
        # Remove any leading/trailing whitespace
        sanitized = sanitized.strip()
        # Remove any leading/trailing underscores
        sanitized = sanitized.strip("_")

        # Ensure the name is not empty
        if not sanitized:
            raise ValueError(f"Invalid column name: {value}")
        # Ensure the column name starts with a letter
        if not sanitized[0].isalpha():
            sanitized = f"col_{sanitized}"

        return sanitized.lower()

    def __repr__(self):
        """pretty print column name

        ex: <column_name: TEXT>
        """
        return f"<{self.name}: {self.data_type}>"


class AbstractDataLoader(ABC):
    """Interface for data loaders."""

    config: DataSourceModel
    model: DomainModel
    handler: IDataHandler
    db_client: IDBClient

    # cache for columns
    columns: list[Column] = None

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
    def list_columns(self) -> list[Column]:
        """List columns to be created in the target table."""
        pass

    def create_or_overwrite_table(self):
        # initiate database session
        logger.info(f"Creating table bronze.{self.model.table_name}")

        try:

            self.db_client.connect()

            try:
                self.db_client.execute(
                    f"DROP TABLE IF EXISTS bronze.{self.model.table_name}"
                )
            except psycopg2.errors.DependentObjectsStillExist as e:
                logger.warning(
                    f"Failed to drop table bronze.{self.model.table_name}: {str(e)}"
                )
                # need to rollback the transaction
                # to reset the state of the database
                # before creating the table
                self.db_client.rollback()

            # cache columns for later
            self.columns = self.list_columns()

            logger.debug(
                f"Creating table bronze.{self.model.table_name} with columns: {self.columns}"
            )

            columns_str = ", ".join(
                [f"{col.name} {col.data_type}" for col in self.columns]
            )

            self.db_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS bronze.{self.model.table_name} (
                    id SERIAL PRIMARY KEY,
                    {columns_str},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) 
            """
            )

            # add comments on table
            self.db_client.execute(
                f"""
                COMMENT ON TABLE bronze.{self.model.table_name} IS %s
                """,
                (self.model.description,),
            )

            # add comments on columns
            for col in self.columns:
                if col.description is not None:
                    self.db_client.execute(
                        f"""
                        COMMENT ON COLUMN bronze.{self.model.table_name}.{col.name} IS %s
                        """,
                        (col.description,),
                    )

            self.db_client.commit()

            logger.info(f"Table bronze.{self.model.table_name} created successfully")

        finally:
            # always close the db connection
            self.db_client.close()

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
                f"Failed to load 'extract' metadata for model '{self.model.name}': {str(e)}"
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

        self.handler.dump_metadata(
            self.model,
            OperationType.LOAD,
            start_time=start_time,
            last_processed_page=last_processed_page,
            complete=complete,
            errors=errors,
            pages=page_logs,
            artifacts=[],
        )
