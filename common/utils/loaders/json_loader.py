from typing import Generator

import jmespath
from psycopg2.extras import Json
from pydantic import validate_call

from common.utils.interfaces.data_handler import PageLog, ArtifactLog
from common.utils.interfaces.loader import AbstractDataLoader
from common.utils.logging_odis import logger


class JsonDataLoader(AbstractDataLoader):

    def create_or_overwrite_table(self, suffix:str = None):
        """Creates the target Bronze table.
        If exists, drops table to recreate"""

        # append suffix to construct final name if provided
        table_name = f"{self.model.table_name}_{suffix}" if suffix else self.model.table_name

        # initiate database session
        logger.info(f"Creating table bronze.{table_name}")

        try:

            self.db_client.connect()

            self.db_client.execute(
                f"DROP TABLE IF EXISTS bronze.{table_name} CASCADE;"
            )
            self.db_client.execute(
                f"""
                CREATE TABLE bronze.{table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
            """
            )
            self.db_client.commit()

            logger.info(f"Table bronze.{table_name} created successfully")

        finally:
            # always close the db connection
            self.db_client.close()

    def load_data(self, pages: list[PageLog]) -> Generator[PageLog, None, None]:
        """Method to load pages from json files, indexed in a DataProcessLog object.
        Yields an iterable result with page number (int) and load success information (bool) for each page
        """

        for extract_page_log in pages:

            try:

                load_success = False

                self.db_client.connect()

                raw_data = self.handler.json_load(extract_page_log.storage_info)

                # get the datapath field in the model definition and get the actual data records
                datapath = jmespath.search(
                    "response_map.data", self.model.model_dump(mode="json")
                )

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

                # Convert single object to list for consistent processing
                if isinstance(payload, dict):
                    payload = [payload]

                load_success = self.load_to_db(payload)

                logger.info(
                    f"Page {extract_page_log.page} loaded successfully: {load_success}"
                )

            except Exception as e:
                logger.exception(
                    f"Error loading data for page {extract_page_log.page}: {str(e)}"
                )
            finally:
                # close db connection
                self.db_client.close()

            # yield a new page log, with the db load result info
            yield PageLog(
                page=extract_page_log.page,
                storage_info=extract_page_log.storage_info,
                success=load_success,
                is_last=extract_page_log.is_last,
            )

    def load_artifacts(self, artifacts: list[ArtifactLog]) -> Generator[ArtifactLog, None, None]:
        """Method to load pages from json files, indexed in a DataProcessLog object.
        Yields an iterable result with page number (int) and load success information (bool) for each page
        """

        # initiate connection
        self.db_client.connect()

        for artifact_log in artifacts:

            try:

                load_success = False

                # Drops if exist artifact table
                self.create_or_overwrite_table(suffix = artifact_log.name)

                raw_data = self.handler.json_load(artifact_log.storage_info)

                # get the datapath field in the model definition and get the actual data records
                datapath = jmespath.search(
                    "response_map.data", self.model.model_dump(mode="json")
                )

                if datapath:
                    payload = jmespath.search(datapath, raw_data)
                else:
                    logger.info(
                        f"did not find a datapath indication for {self.model.name}: Loading JSON data as-is."
                    )
                    payload = raw_data

                logger.info(
                    f"Inserting artifact table {artifact_log.name} from model {self.model.table_name}"
                )

                # Convert single object to list for consistent processing
                if isinstance(payload, dict):
                    payload = [payload]

                load_success = self.load_to_db(payload, suffix = artifact_log.name)

                logger.info(
                    f"Page {artifact_log.name} loaded successfully: {load_success}"
                )

            except Exception as e:
                logger.exception(
                    f"Error loading data for page {artifact_log.name}: {str(e)}"
                )

            # yield a new page log, with the db load result info
            yield ArtifactLog(
                name = artifact_log.name,
                storage_info = artifact_log.storage_info,
                success = load_success,
                load_to_bronze = artifact_log.load_to_bronze,
            )
    
        # close db connection
        self.db_client.close()

    @validate_call
    def load_to_db(self, rows: list[dict], suffix:str = None) -> bool:
        """
        Load list(dict) data records into the model table in the database

        Args:
            rows (list[dict]: list of data records to be loaded into the database
        Returns:
            bool: True if the data loading is successful, False otherwise

        """
        # append suffix to construct final name if provided
        table_name = f"{self.model.table_name}_{suffix}" if suffix else self.model.table_name
        # insert Data
        insert_query = f"INSERT INTO bronze.{table_name} (data) VALUES (%s)"

        # unpack the rows and convert them to JSON
        # each row is a line in the table
        self.db_client.executemany(insert_query, [(Json(row),) for row in rows])
        self.db_client.commit()

        logger.info(
            f"Successfully Loaded {len(rows)} data into '{table_name}'"
        )

        return True
