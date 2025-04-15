import csv
import re
import unicodedata
from io import StringIO
from pathlib import Path
from typing import Generator

import pandas as pd

from common.utils.exceptions import InvalidCSV
from common.utils.interfaces.data_handler import OperationType, PageLog
from common.utils.interfaces.loader import AbstractDataLoader
from common.utils.logging_odis import logger


class CsvDataLoader(AbstractDataLoader):

    columns: list[str] = []

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
                f"DROP TABLE IF EXISTS bronze.{table_name}"
            )

            # load actual data from metadata
            metadata = self.handler.load_metadata(
                self.model, operation=OperationType.EXTRACT
            )

            # take 1st page log
            page_log = metadata.pages[0]

            self.columns = [
                self._sanitize_column_name(col) for col in self._snif_columns(page_log)
            ]
            columns_str = ", ".join([f"{col} TEXT" for col in self.columns])

            self.db_client.execute(
                f"""
                CREATE TABLE bronze.{table_name} (
                    id SERIAL PRIMARY KEY,
                    {columns_str},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            self.db_client.commit()

            logger.info(f"Table bronze.{table_name} created successfully")

        finally:
            # always close the db connection
            self.db_client.close()

    def load_data(self, pages: list[PageLog]) -> Generator[PageLog, None, None]:
        """
        Load CSV data into the database

        Args:
            pages (list[PageLog]): List of PageLog objects containing page information and storage details
        """

        load_success = False

        for extract_page_log in pages:

            try:

                # snif columns if not already done
                if not self.columns:
                    self.columns = [
                        self._sanitize_column_name(col)
                        for col in self._snif_columns(extract_page_log)
                    ]

                self.db_client.connect()

                # Read CSV with specific parameters from config
                results = self.handler.csv_load(extract_page_log.storage_info, self.model)

                for _, row in results.iterrows():
                    # Convert any NaN values to None for database compatibility
                    values = [None if pd.isna(val) else str(val) for val in row]
                    placeholders = ", ".join(["%s"] * len(values))
                    column_names = ", ".join([f'"{col}"' for col in self.columns])

                    self.db_client.execute(
                        f"""
                        INSERT INTO bronze.{self.model.table_name} ({column_names}, created_at)
                        VALUES ({placeholders}, CURRENT_TIMESTAMP)
                    """,
                        values,
                    )

                self.db_client.commit()
                logger.info(
                    f"Successfully loaded {len(results)} rows for '{self.model.name}'"
                )

                load_success = True

            except Exception as e:

                logger.exception(
                    f"Error loading CSV data for '{self.model.name}': {str(e)}"
                )

            finally:
                # always close the db connection
                self.db_client.close()

            # yield a new page log, with the db load result info
            yield PageLog(
                page=extract_page_log.page,
                storage_info=extract_page_log.storage_info,
                success=load_success,
                is_last=extract_page_log.is_last,
            )

    def _sanitize_column_name(self, column_name: str) -> str:
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
            unicodedata.normalize("NFKD", column_name)
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
            raise ValueError(f"Invalid column name: {column_name}")
        # Ensure the column name starts with a letter
        if not sanitized[0].isalpha():
            sanitized = f"col_{sanitized}"

        return sanitized.lower()

    def _snif_columns(self, page_log: PageLog) -> list[str]:
        """
        Sniff the CSV file to determine the column names and types.
        This method is used to create the table dynamically based on the CSV structure.

        Example:
        ```
        # imagine the CSV file has the following content:
        # "ANNEE";"DEPARTEMENT_CODE";"DEPARTEMENT_LIBELLE";"TYPE_LGT";"LOG_AUT";"LOG_COM";"SDP_AUT";"SDP_COM"
        # "2023";"01";"Ain";"Tous Logements";4586;;409166;
        # "2023";"01";"Ain";"Individuel pur";1273;;159228;

        # The sniffed columns would be: ['ANNEE', 'DEPARTEMENT_CODE', 'DEPARTEMENT_LIBELLE',...]
        ```
        """

        # load few data in the CSV file
        filepath = Path(page_log.storage_info.location) / Path(
            page_log.storage_info.file_name
        )

        with open(
            filepath, "r", encoding=page_log.storage_info.encoding
        ) as original_csv:

            lines = original_csv.readlines()

            total_len = len(lines)

            lines = lines[
                self.model.load_params.header : total_len
                - self.model.load_params.skipfooter
            ]

            stream = StringIO("".join(lines))

            # take into account the separator if defined in the model
            delimiter = None

            if self.model.load_params and self.model.load_params.separator:
                delimiter = self.model.load_params.separator

            dialect = csv.Sniffer().sniff(stream.getvalue(), delimiters=delimiter)

            if delimiter is None:
                # If no delimiter is provided, use the default dialect delimiter
                # This is a fallback in case the model does not specify a separator
                delimiter = dialect.delimiter

            # Reset file pointer to the beginning
            stream.seek(0)

            csv_reader = csv.DictReader(stream, dialect=dialect, delimiter=delimiter)

            if csv_reader.fieldnames is None:
                logger.error(f"No field names found in CSV file: {filepath}")
                raise InvalidCSV(
                    f"Invalid CSV file: No field names found in {filepath}"
                )

            logger.info(f"Sniffed columns: {csv_reader.fieldnames}")

            return csv_reader.fieldnames
