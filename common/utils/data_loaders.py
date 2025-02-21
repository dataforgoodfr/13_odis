from abc import ABC, abstractmethod
from pathlib import Path
from common.utils.database_client import DatabaseClient
from psycopg2.extras import Json
import json

class DataLoader(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'load' method is mandatory, which is responsible
    for loading the data in a database.
    """
    @abstractmethod
    def load(self, domain: str, source_config):
        pass

class JsonDataLoader(DataLoader):
    def load(self, domain: str, source_config):
        dataset_name = source_config['name']

        # read data
        file_path = Path(f"data/imports/{domain}/{dataset_name}.json")
        if not file_path.exists():
                raise FileNotFoundError(f"JSON file not found at {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON format in {file_path}: {str(e)}", e.doc, e.pos) from e
        except Exception as e:
            raise IOError(f"Error reading file {file_path}: {str(e)}") from e
        
        # Validate data structure
        if not isinstance(data, (list, dict)):
            raise ValueError("JSON data must be either a list or dictionary")

        # Convert single object to list for consistent processing
        if isinstance(data, dict):
            data = [data]

        # create bronze table drop if it already exists
        table_name = f"{domain}_{dataset_name}"
        try:
            db = DatabaseClient(autocommit=False)
            db.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
            db.execute(f"""
                CREATE TABLE bronze.{table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
            """)
            db.commit()
            # insert Data
            insert_query = f"INSERT INTO bronze.{table_name} (data) VALUES (%s)"
            for record in data:
                db.execute(insert_query, (Json(record),))
            db.commit()
        except Exception as e:
            raise Exception(f"Database operation failed: {str(e)}") from e

        # close db connection
        db.close()
