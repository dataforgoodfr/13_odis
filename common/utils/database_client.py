import psycopg2


class DatabaseClient:

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

        self.connection = psycopg2.connect(
            dbname=settings.get("PG_DB_NAME"),
            user=settings.get("PG_DB_USER"),
            password=settings.get("PG_DB_PWD"),
            host=settings.get("PG_DB_HOST"),
            port=settings.get("PG_DB_PORT"),
        )
        self.connection.autocommit = autocommit
        self.cursor = self.connection.cursor()

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
