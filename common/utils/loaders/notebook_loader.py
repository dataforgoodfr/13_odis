from pathlib import Path
from typing import Generator
# import datetime

import papermill as pm

from common.utils.interfaces.data_handler import PageLog
from common.utils.interfaces.loader import AbstractDataLoader
from common.utils.logging_odis import logger


class NotebookLoader(AbstractDataLoader):

    columns: list[str] = []

    def create_or_overwrite_table(self):
        pass

    def list_columns(self):
        pass

    def load_data(self, pages: list[PageLog]) -> Generator[PageLog, None, None]:
        """
        Pass XLSX file info to a Notebook for preprocessing (and loading)

        Args:
            pages (list[PageLog]): List of PageLog objects containing page information and storage details
        """

        load_success = False
        # start_time = datetime.datetime.now(tz=datetime.timezone.utc)

        for extract_page_log in pages:

            try:
                
                # Prepare Notebook parameters
                input_path = Path(extract_page_log.storage_info.location) 
                input_filepath = input_path / extract_page_log.storage_info.file_name

                notebook_name = self.model.preprocessor.name
                base_path = self.model.preprocessor.base_path
                notebook_path = base_path / f"{notebook_name}.ipynb"
                output_notebook_path = base_path / f"{notebook_name}_processed.ipynb"

                params = {
                    'filepath' : str(input_filepath),
                    'model_name' : self.model.name,
                    # 'model': self.model,
                    # 'handler': self.handler,
                    # 'start_time': start_time
                }

                # Execute notebook
                logger.info(f"Executing notebook: {notebook_path}")
                pm.execute_notebook(
                    notebook_path,
                    output_notebook_path,
                    parameters = params
                )

                load_success = True

            except Exception as e:

                logger.exception(
                    f"Error processing XLSX data for '{self.model.name}': {str(e)}"
                )

            # yield a new page log, with the db load result info
            yield PageLog(
                page=extract_page_log.page,
                storage_info=extract_page_log.storage_info,
                success=load_success,
                is_last=extract_page_log.is_last,
            )
