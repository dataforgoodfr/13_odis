import re
import unicodedata
from typing import Generator

import jmespath
import pandas as pd
from psycopg2.extras import Json
from pydantic import validate_call

from common.utils.database_client import DatabaseClient
from common.utils.interfaces.data_handler import PageLog
from common.utils.interfaces.data_loader import AbstractDataLoader
from common.utils.logging_odis import logger


class JsonDataLoader(AbstractDataLoader):

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

    def load_data(self, pages: list[PageLog]) -> Generator[PageLog, None, None]:
        """Method to load pages from json files, indexed in a DataProcessLog object.
        Yields an iterable result with page number (int) and load success information (bool) for each page
        """

        for extract_page_log in pages:

            raw_data = self.handler.json_load(extract_page_log)

            # get the datapath field in the model definition and get the actual data records
            datapath = jmespath.search(
                "response_map.data", self.model.model_dump(mode="json")
            )
            logger.debug(f"Datapath: {datapath}")
            if datapath:
                payload = jmespath.search(datapath, raw_data)
            else:
                logger.info(
                    f"did not find a datapath indication for {self.model.name}: Loading JSON data as-is."
                )
                payload = raw_data

            logger.info(
                f"Inserting page {extract_page_log.page} into {self.model.table_name}"
            )
            load_success = self.load_to_db(payload)

            # yield a new page log, with the db load result info
            yield PageLog(
                page=extract_page_log.page,
                storage_info=extract_page_log.storage_info,
                success=load_success,
                is_last=extract_page_log.is_last,
            )

    @validate_call
    def load_to_db(self, payload: list[dict] | dict) -> bool:
        """Load list(dict)-type data records into the database with table_name = 'domain_source'

        TODO:
            - testing
        """

        # Convert single object to list for consistent processing
        if isinstance(payload, dict):
            payload = [payload]

        load_success = False

        try:
            # initiate database session
            db = DatabaseClient(autocommit=False)

            # insert Data
            insert_query = (
                f"INSERT INTO bronze.{self.model.table_name} (data) VALUES (%s)"
            )
            for record in payload:
                # Uncomment if needed ; very verbose :)
                # logger.debug(f"Inserting record: {record}")
                db.execute(insert_query, (Json(record),))
            db.commit()

            load_success = True
            logger.info(f"Successfully Loaded: {self.model.table_name}")

        except Exception as e:
            logger.exception(f"Database operation failed: {e}")

        # close db connection
        db.close()

        return load_success


class CsvDataLoader(AbstractDataLoader):

    def create_or_overwrite_table(self):
        """Creates the target Bronze table.
        If exists, drops table to recreate"""

        # domain_name = self.config.get_domain_name(self.model)

        # initiate database session
        db = DatabaseClient(autocommit=False)

        # create bronze table drop if it already exists
        # table_name = f"{domain_name}_{self.model.table_name}"

        logger.info(f"Dropping table bronze.{self.model.table_name}")
        db.execute(f"DROP TABLE IF EXISTS bronze.{self.model.table_name}")
        db.commit()

    def load_data(self, pages:list[PageLog]) -> Generator[PageLog, None, None]:
        """
        Load CSV data into the database

        TODO:
            - improve error catching behaviour
            - improve output load metadata : save it as json, not csv
        """
        db = DatabaseClient()

            # Read data extraction log provided by extractor
            # data_index_name = f"{source_name}_extract_log"
            # data_index = self.handler.json_load(domain=domain, source_name=data_index_name)
            # csv_file_path = data_index["pages"][0]["filepath"]

        load_success = False

        for extract_page_log in pages:

            try:

                # Read CSV with specific parameters from config
                if self.model.load_params:
                    results = self.handler.csv_load(
                        extract_page_log,
                        **self.model.load_params
                    )
                else:
                    results = self.handler.csv_load(
                        extract_page_log
                    )

                results.columns = [
                    self._sanitize_column_name(col) for col in results.columns
                ]
                columns = [f"{col} TEXT" for col in results.columns]
                columns_str = ", ".join(columns)

                db.execute(
                    f"""
                    CREATE TABLE bronze.{self.model.table_name} (
                        id SERIAL PRIMARY KEY,
                        {columns_str},
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                for _, row in results.iterrows():
                    # Convert any NaN values to None for database compatibility
                    values = [None if pd.isna(val) else str(val) for val in row]
                    placeholders = ", ".join(["%s"] * len(values))
                    column_names = ", ".join([f'"{col}"' for col in results.columns])

                    db.execute(
                        f"""
                        INSERT INTO bronze.{self.model.table_name} ({column_names}, created_at)
                        VALUES ({placeholders}, CURRENT_TIMESTAMP)
                    """,
                        values,
                    )

                db.commit()
                logger.info(
                    f"Successfully loaded {len(results)} rows for '{self.model.name}'"
                )

                load_success = True

            except Exception as e:
                # db.rollback()
                logger.exception(f"Error loading CSV data for '{self.model.name}': {str(e)}")
                # raise

            # yield a new page log, with the db load result info
            yield PageLog(
                page=extract_page_log.page,
                storage_info=extract_page_log.storage_info,
                success=load_success,
                is_last=extract_page_log.is_last,
            )

            db.close()

    def _sanitize_column_name(self, column_name: str) -> str:
        """
        Sanitize column names by:
        1. Replacing spaces with underscores
        2. Removing accents
        3. Ensuring the name is SQL-friendly
        """
        # Normalize unicode characters and remove accents
        normalized = (
            unicodedata.normalize("NFKD", column_name)
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
        # Replace spaces with underscores
        sanitized = normalized.replace(" ", "_")
        # Remove any non-alphanumeric characters except underscores
        sanitized = re.sub(r"[^\w]", "", sanitized)
        # Ensure the column name starts with a letter
        if not sanitized[0].isalpha():
            sanitized = f"col_{sanitized}"

        return sanitized.lower()
