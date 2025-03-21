from abc import ABC, abstractmethod
from psycopg2.extras import Json
import jmespath

from common.utils.logging_odis import logger
from common.utils.database_client import DatabaseClient
from common.utils.file_handler import FileHandler
from common.data_source_model import DataProcessLog

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

    def init_jsontable(self, table_name:str, schema:str = 'bronze'):
        """Method to drop a JSON data table if it exists in the schema"""

        success = False

        try:
            # initiate database session
            db = DatabaseClient(autocommit=False)

            # create bronze table drop if it already exists
            logger.info(f"Dropping table (if exists): {schema}.{table_name}")
            db.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")

            logger.info(f"Creating table: {schema}.{table_name}")
            db.execute(f"""
                CREATE TABLE {schema}.{table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
            """)
            db.commit()
            db.close()
            
            success = True
            
            return success

        except Exception as e:
            
            logger.exception(f"Could not reinitialize table {schema}.{table_name}: {e}")
            return success
    
    def load_pagelogs(self, process_log:DataProcessLog):

        for pageno, pagelog in process_log.pages.items():

            table_name = f"{process_log.domain}_{process_log.source}"
            logger.info(f"Inserting page {pageno} into {table_name}")

            load_success = self.load_from_file(
                pagelog.filepath,
                process_log.domain,
                process_log.source,
                process_log.source_config
            )

            yield pageno, load_success


    def load_from_file(self, filepath:str, domain:str, source_name:str, source_config:dict = None):

        raw_data = self.fh.json_load(filepath = filepath)

        # get the datapath field and get the actual data records
        datapath = jmespath.search('response_map.data', source_config)
        logger.debug(f"Datapath: {datapath}")
        
        if datapath:
            payload = jmespath.search(datapath, raw_data)

        else:
            logger.info(f"did not find a datapath indication for {domain}_{source_name}: Loading JSON data as-is.")
            payload = raw_data

        load_success = self.load(payload, domain, source_name)

        return load_success

    def load(self, payload, domain: str, source_name: str):

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
                db.execute(insert_query, (Json(record),))
            db.commit()

            load_success = True
            logger.info(f"Successfully Loaded: {domain}/{source_name}")

        except Exception as e:
            logger.exception(f"Database operation failed: {e}")

        # close db connection
        db.close()

        return load_success


