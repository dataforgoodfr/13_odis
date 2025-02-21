import os
import psycopg2

class DatabaseClient():
    def __init__(self, autocommit=True):
        self.connection = psycopg2.connect(
            dbname=os.getenv('PG_DB_NAME'),
            user=os.getenv('PG_DB_USER'),
            password=os.getenv('PG_DB_PWD'),
            host=os.getenv('PG_DB_HOST'),
            port=os.getenv('PG_DB_PORT')
        )
        self.connection.autocommit = autocommit
        self.cursor = self.connection.cursor()

    def execute(self, query, params=None):
        try:
            self.cursor.execute(query, params)
        except Exception as e:
            print(f"An error occurred: {e}")

    def commit(self):
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

# Example usage:
# db_client = DatabaseClient()
# db_client.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(50), age INT)")
# db_client.close()