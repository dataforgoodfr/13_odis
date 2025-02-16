import os
import psycopg2

class DatabaseClient:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=os.environ['DB_PG_NAME'],
            user=os.environ['DB_PG_USER'],
            password=os.environ['DB_PG_PWD'],
            host=os.environ['DB_PG_HOST'],
            port=os.environ['DB_PG_PORT']
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def execute(self, query, params=None):
        try:
            self.cursor.execute(query, params)
        except Exception as e:
            print(f"An error occurred: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()

# Example usage:
# db_client = DatabaseClient()
# db_client.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(50), age INT)")
# db_client.close()