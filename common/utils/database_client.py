import psycopg2

from common.utils.interfaces.db_client import IDBClient


class DatabaseClient(IDBClient):

    cursor: psycopg2.extensions.cursor
    connection: psycopg2.extensions.connection
    settings: dict
    autocommit: bool

    def __init__(
        self,
        settings: dict,
        autocommit=True,
    ):
        """
        Args:
            settings (dict): Database connection settings.
            autocommit (bool, optional): Defaults to True.
        """
        super().__init__(
            settings,
            autocommit,
        )

        self.settings = settings
        self.autocommit = autocommit

        self.connect()

    def execute(self, query, params=None):
        self.cursor.execute(query, params)

    def executemany(self, query, params):
        self.cursor.executemany(query, params)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def rollback(self):
        self.connection.rollback()

    def connect(self):

        if not self.is_alive():
            self.connection = psycopg2.connect(
                dbname=self.settings.get("PG_DB_NAME"),
                user=self.settings.get("PG_DB_USER"),
                password=self.settings.get("PG_DB_PWD"),
                host=self.settings.get("PG_DB_HOST"),
                port=self.settings.get("PG_DB_PORT"),
            )
            self.connection.autocommit = self.autocommit
            self.cursor = self.connection.cursor()

    def is_alive(self) -> bool:
        """
        Check if the database connection is alive.
        Returns:
            bool: True if the connection is alive, False otherwise.
        """
        try:
            self.cursor.execute("SELECT 1")
            return True
        except (psycopg2.Error, AttributeError):
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.close()
