from abc import ABC, abstractmethod
from psycopg2.extras import Json

import pandas as pd
import unicodedata
import re

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
        data_index_name = f"{source_name}_extract_log"
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
        for item in data_index.get('pages'):
            
            pageno = item.get('pageno')
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
                    db.execute(insert_query, (Json(record),))
                db.commit()

                logger.info(f"Successfully Loaded: {domain}/{source_name}")

            except Exception as e:
                raise Exception(f"Database operation failed: {str(e)}") from e

        # close db connection
        db.close()

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