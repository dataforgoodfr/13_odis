import csv
from io import StringIO
from pathlib import Path
from typing import Generator

import pandas as pd

from common.utils.exceptions import InvalidCSV
from common.utils.interfaces.data_handler import OperationType, PageLog
from common.utils.interfaces.loader import AbstractDataLoader, Column, ColumnType
from common.utils.logging_odis import logger


class CsvDataLoader(AbstractDataLoader):

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
                    self.columns = self.list_columns()

                self.db_client.connect()

                # Read CSV with specific parameters from config
                results = self.handler.csv_load(
                    extract_page_log.storage_info, self.model
                )

                for _, row in results.iterrows():
                    # Convert any NaN values to None for database compatibility
                    values = [None if pd.isna(val) else str(val) for val in row]
                    placeholders = ", ".join(["%s"] * len(values))
                    column_names = ", ".join([f'"{col.name}"' for col in self.columns])

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

    def list_columns(self) -> list[Column]:
        """
        Sniff the first CSV page log file to determine the column names

        All columns are set to TEXT type and nullable.

        The dictionary is used to provide a description for each column.

        Example:
        ```
        # imagine the CSV file has the following content:
        # "ANNEE";"DEPARTEMENT_CODE";"DEPARTEMENT_LIBELLE";"TYPE_LGT";"LOG_AUT";"LOG_COM";"SDP_AUT";"SDP_COM"
        # "2023";"01";"Ain";"Tous Logements";4586;;409166;
        # "2023";"01";"Ain";"Individuel pur";1273;;159228;

        # The sniffed columns would be: ['annee', 'departement_code',...]
        ```
        """

        metadata = self.handler.load_metadata(
            self.model, operation=OperationType.EXTRACT
        )

        # take 1st page log
        page_log = metadata.pages[0]

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

            logger.debug(f"Sniffed columns: {csv_reader.fieldnames}")

            return [
                Column(
                    name=col,
                    data_type=ColumnType.TEXT,
                    description="",  # self.model.dictionary.get(col, ""),
                )
                for col in csv_reader.fieldnames
                # if col not in self.model.load_params.ignore_columns  # TODO: add this to the model
            ]
