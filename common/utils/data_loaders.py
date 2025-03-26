import re
import unicodedata

import jmespath
import pandas as pd
from psycopg2.extras import Json

from common.utils.database_client import DatabaseClient
from common.utils.interfaces.data_loader import AbstractDataLoader
from common.utils.logging_odis import logger
from common.data_source_model import DataProcessLog


class JsonDataLoader(AbstractDataLoader):

    def load_data(self):

        domain_name = self.config.get_domain_name(self.model)

        # initiate database session
        db = DatabaseClient(autocommit=False)

        # create bronze table drop if it already exists
        table_name = f"{domain_name}_{self.model.table_name}"

        logger.info(f"Creating table bronze.{table_name}")

        db.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
        db.execute(
            f"""
            CREATE TABLE bronze.{table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        """
        )
        db.commit()

        # load actual data from metadata
        metadata_info = self.load_metadata()
        for item in metadata_info.file_dumps:

            page_data = self.handler.json_load(item)

            # Validate data structure
            if not isinstance(page_data, (list, dict)):
                raise ValueError("JSON data must be either a list or dictionary")

            # Convert single object to list for consistent processing
            if isinstance(page_data, dict):
                page_data = [page_data]

            try:
                logger.info(f"Inserting page {item.page} into {table_name}")
                # insert Data
                insert_query = f"INSERT INTO bronze.{table_name} (data) VALUES (%s)"
                for record in page_data:
                    db.execute(insert_query, (Json(record),))
                db.commit()

                logger.info(f"Successfully Loaded: {domain_name}/{self.model.name}")

            except Exception as e:
                raise Exception(f"Database operation failed: {str(e)}") from e

        # close db connection
        db.close()

    def load_pagelogs(self, process_log: DataProcessLog):
        """Method to load pages from json files, indexed in a DataProcessLog object.
        Yields an iterable result with page number (int) and load success information (bool) for each page
        """

        for pageno, pagelog in process_log.pages.items():

            table_name = f"{process_log.domain}_{process_log.source}"
            logger.info(f"Inserting page {pageno} into {table_name}")

            load_success = self.load_from_file(
                pagelog.filepath,
                process_log.domain,
                process_log.source,
                process_log.source_config,
            )

            yield pageno, load_success

    def load_from_file(
        self, filepath: str, domain: str, source_name: str, source_config: dict = None
    ):
        """Imports a JSON file and loads it to the database"""

        raw_data = self.handler.json_load(filepath=filepath)

        # get the datapath field and get the actual data records
        datapath = jmespath.search("response_map.data", source_config)
        logger.debug(f"Datapath: {datapath}")

        if datapath:
            payload = jmespath.search(datapath, raw_data)

        else:
            logger.info(
                f"did not find a datapath indication for {domain}_{source_name}: Loading JSON data as-is."
            )
            payload = raw_data

        load_success = self.load(payload, domain, source_name)

        return load_success

    def load(self, payload, domain: str, source_name: str):
        """Load list(dict)-type data records into the database with table_name = 'domain_source'"""

        # Validate data structure
        if not isinstance(payload, (list, dict)):
            raise ValueError("JSON data must be either a list or dictionary")

        # Convert single object to list for consistent processing
        if isinstance(payload, dict):
            payload = [payload]

        load_success = False

        try:
            # initiate database session
            db = DatabaseClient(autocommit=False)
            table_name = f"{domain}_{source_name}"

            # insert Data
            insert_query = f"INSERT INTO bronze.{table_name} (data) VALUES (%s)"
            for record in payload:
                # Uncomment if needed ; very verbose :)
                # logger.debug(f"Inserting record: {record}")
                db.execute(insert_query, (Json(record),))
            db.commit()

            load_success = True
            logger.info(f"Successfully Loaded: {domain}/{source_name}")

        except Exception as e:
            logger.exception(f"Database operation failed: {e}")

        # close db connection
        db.close()

        return load_success


class CsvDataLoader(AbstractDataLoader):

    def load_data(self):
        """
        Load CSV data into the database

        TODO:
            - improve DomainModel to include CSV-specific parameters (header, skipfooter, separator)
            - refacto and testing
        """

        domain_name = self.config.get_domain_name(self.model)
        # Extract CSV-specific parameters
        header = self.model.load_params.get("header", None)
        skipfooter = self.model.load_params.get("skipfooter", None)
        separator = self.model.load_params.get("separator", ",")

        db = DatabaseClient(autocommit=False)

        if not header or not skipfooter:
            logger.error(
                f"The source '{self.model.name}' needs to have params 'header' and 'skipfooter' configured"
            )
            exit()

        # Create bronze table, drop if it already exists
        table_name = f"{domain_name}_{self.model.table_name}"
        db.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")

        try:

            # Read data extraction log provided by extractor
            # data_index_name = f"{source_name}_extract_log"
            # data_index = self.handler.json_load(domain=domain, source_name=data_index_name)
            # csv_file_path = data_index["pages"][0]["filepath"]
            metadata_info = self.load_metadata()
            for item in metadata_info.file_dumps:

                # Read CSV with specific parameters from config
                results = self.handler.csv_load(
                    item,
                    header=header,
                    skipfooter=skipfooter,
                    separator=separator,
                )

                results.columns = [
                    self._sanitize_column_name(col) for col in results.columns
                ]
                columns = [f"{col} TEXT" for col in results.columns]
                columns_str = ", ".join(columns)

                db.execute(
                    f"""
                    CREATE TABLE bronze.{table_name} (
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
                        INSERT INTO bronze.{table_name} ({column_names}, created_at)
                        VALUES ({placeholders}, CURRENT_TIMESTAMP)
                    """,
                        values,
                    )

                db.commit()
                logger.info(
                    f"Successfully loaded {len(results)} rows for '{self.model.name}'"
                )

        except Exception as e:
            db.rollback()
            logger.error(f"Error loading CSV data for '{self.model.name}': {str(e)}")
            raise

        finally:
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
