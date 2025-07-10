import pandas as pd
from pathlib import Path
from typing import Generator

from common.utils.exceptions import InvalidExcel
from common.utils.interfaces.data_handler import OperationType, PageLog
from common.utils.interfaces.loader import AbstractDataLoader, Column, ColumnType
from common.utils.logging_odis import logger


class XlsxDataLoader(AbstractDataLoader):

    def load_data(self, pages: list[PageLog]) -> Generator[PageLog, None, None]:
        """
        Load XLSX data into the database.
        This assumes only the first sheet of the xlsx file is loaded ; 
        for morecomplex scenarios a Notebook appraoch is needed
        """
        load_success = False

        for extract_page_log in pages:
            try:
                if not self.columns:
                    self.columns = self.list_columns()
                    logger.debug(f"Columns detected for model '{self.model.name}': {[col.name for col in self.columns]}")

                self.db_client.connect()

                # Load Excel data using handler
                results = self.handler.xlsx_load(
                    extract_page_log.storage_info, self.model
                )

                # Supprimer les colonnes dupliquées
                results = results.loc[:, ~results.columns.duplicated()]
                logger.debug(f"DataFrame columns after deduplication: {list(results.columns)}")

                # Vérification stricte de l'alignement
                if len(results.columns) != len(self.columns):
                    raise ValueError(
                        f"Mismatch between DataFrame columns ({len(results.columns)}) and model columns ({len(self.columns)})."
                    )

                for _, row in results.iterrows():
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
                    f"Error loading XLSX data for '{self.model.name}': {str(e)}"
                )

            finally:
                self.db_client.close()

            yield PageLog(
                page=extract_page_log.page,
                storage_info=extract_page_log.storage_info,
                success=load_success,
                is_last=extract_page_log.is_last,
            )

    def list_columns(self) -> list[Column]:
        """
        Sniff the first file to determine the column names.
        Deduplicates columns while preserving order,
        after applying sanitization (accents, lowercase, etc).
        """

        metadata = self.handler.load_metadata(
            self.model, operation=OperationType.EXTRACT
        )
        page_log = metadata.pages[0]
        filepath = Path(page_log.storage_info.location) / Path(
            page_log.storage_info.file_name
        )

        df = pd.read_excel(
            filepath,
            header=self.model.load_params.header,
            skipfooter=self.model.load_params.skipfooter,
            engine="openpyxl"
        )

        seen = set()
        deduped_columns = []
        for original_col in df.columns:
            try:
                sanitized = Column(name=original_col).name
            except Exception as e:
                logger.warning(f"Skipping invalid column '{original_col}': {e}")
                continue

            if sanitized not in seen:
                seen.add(sanitized)
                deduped_columns.append(Column(name=sanitized, data_type=ColumnType.TEXT))
            else:
                logger.warning(f"Duplicate column after sanitation: '{sanitized}' (original: '{original_col}'). It will be ignored.")

        logger.debug(f"Final deduplicated column names: {[col.name for col in deduped_columns]}")

        return deduped_columns