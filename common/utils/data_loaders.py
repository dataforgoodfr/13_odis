from psycopg2.extras import Json

from common.utils.database_client import DatabaseClient
from common.utils.interfaces.data_loader import AbstractDataLoader
from common.utils.logging_odis import logger


class JsonDataLoader(AbstractDataLoader):

    def load_data(self):

        domain_name = self.config.get_domain_name(self.model)

        # initiate database session
        db = DatabaseClient(autocommit=False)

        # create bronze table drop if it already exists
        table_name = f"{domain_name}_{self.model.name}"
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

            filepath = item.storage_info.location / item.storage_info.file_name

            page_data = self.handler.json_load(filepath=filepath)

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
