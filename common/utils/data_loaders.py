from abc import ABC, abstractmethod
from psycopg2.extras import Json


from common.utils.logging_odis import logger
from common.utils.database_client import DatabaseClient
from common.utils.file_handler import FileHandler

class DataLoader(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'load' method is mandatory, which is responsible
    for loading the data in a database.
    """
    @abstractmethod
    def load(self, domain: str, source_config):
        pass

class JsonDataLoader(DataLoader):

    fh: FileHandler

    def __init__(self):
        
        self.fh = FileHandler()
        super().__init__()

    def load(self, domain: str, source_name: str):

        # read data extraction log provided by extractor
        data_index_name = f"{source_name}_metadata"
        data_index = self.fh.json_load(domain = domain, source_name = data_index_name)

        # initiate database session
        db = DatabaseClient(autocommit=False)
    
        # create bronze table drop if it already exists
        table_name = f"{domain}_{source_name}"
        db.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
        db.execute(f"""
            CREATE TABLE bronze.{table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        """)
        db.commit()

        # load actual data:
        for item in data_index.get('file_dumps'):
            
            pageno = item.get('page')
            filepath = item.get('filepath')

            page_data = self.fh.json_load(filepath = filepath)
        
            # Validate data structure
            if not isinstance(page_data, (list, dict)):
                raise ValueError("JSON data must be either a list or dictionary")

            # Convert single object to list for consistent processing
            if isinstance(page_data, dict):
                page_data = [page_data]

            try:
                logger.info(f"Inserting page {pageno} into {table_name}")
                # insert Data
                insert_query = f"INSERT INTO bronze.{table_name} (data) VALUES (%s)"
                for record in page_data:
                    logger.debug(f"Executing query: {insert_query}")
                    db.execute(insert_query, (Json(record),))
                db.commit()

                logger.info(f"Successfully Loaded: {domain}/{source_name}")

            except Exception as e:
                raise Exception(f"Database operation failed: {str(e)}") from e

        # close db connection
        db.close()
