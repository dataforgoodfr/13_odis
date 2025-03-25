from abc import ABC, abstractmethod
from psycopg2.extras import Json
import pandas as pd
import unicodedata
import re
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
        """Initialize or Reinitialize a table in the given database schema.
        Default schema is 'bronze'.
        
        This method drops the table if present, and then re-creates the table from scratch"""

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
        """Method to load pages from json files, indexed in a DataProcessLog object.
        Yields an iterable result with page number (int) and load success information (bool) for each page"""

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
        """Imports a JSON file and loads it to the database"""

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


class CsvDataLoader(DataLoader):

    fh: FileHandler

    def __init__(self):
        self.fh = FileHandler()
        super().__init__()

    def load(self, domain: str, source_name: str, params: dict):
        # Extract CSV-specific parameters
        header = params.get('header', None)
        skipfooter = params.get('skipfooter', None)
        separator =  params.get('separator', ',')

        if not header or not skipfooter:
            logger.error(f"The source {domain}/{source_name} needs to have params 'header' and 'skipfooter' configured")
            exit()
        
        # Read data extraction log provided by extractor
        data_index_name = f"{source_name}_extract_log"
        data_index = self.fh.json_load(domain=domain, source_name=data_index_name)
        csv_file_path = data_index['pages'][0]['filepath']
        
        db = DatabaseClient(autocommit=False)
        
        # Create bronze table, drop if it already exists
        table_name = f"{domain}_{source_name}"
        db.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
        
        try:
            # Read CSV with specific parameters from config
            df = pd.read_csv(
                csv_file_path,
                header=header,
                skipfooter=skipfooter,
                sep=separator,
                engine='python'  # Required for skipfooter parameter
            )
            
            df.columns = [self._sanitize_column_name(col) for col in df.columns]
            columns = [f"{col} TEXT" for col in df.columns]
            columns_str = ", ".join(columns)
            
            db.execute(f"""
                CREATE TABLE bronze.{table_name} (
                    id SERIAL PRIMARY KEY,
                    {columns_str},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            for _, row in df.iterrows():
                # Convert any NaN values to None for database compatibility
                values = [None if pd.isna(val) else str(val) for val in row]
                placeholders = ", ".join(["%s"] * len(values))
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                
                db.execute(f"""
                    INSERT INTO bronze.{table_name} ({column_names}, created_at)
                    VALUES ({placeholders}, CURRENT_TIMESTAMP)
                """, values)
            
            db.commit()
            logger.info(f"Successfully loaded {len(df)} rows for {domain}/{source_name}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error loading CSV data for {domain}/{source_name}: {str(e)}")
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
        normalized = unicodedata.normalize('NFKD', column_name).encode('ascii', 'ignore').decode('utf-8')
        # Replace spaces with underscores
        sanitized = normalized.replace(' ', '_')
        # Remove any non-alphanumeric characters except underscores
        sanitized = re.sub(r'[^\w]', '', sanitized)
        # Ensure the column name starts with a letter
        if not sanitized[0].isalpha():
            sanitized = f'col_{sanitized}'
        
        return sanitized.lower()