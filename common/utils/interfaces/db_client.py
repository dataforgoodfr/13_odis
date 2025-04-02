from typing import Protocol


class IDBClient(Protocol):

    is_autocommit: bool
    settings: dict

    def __init__(
        self,
        settings: dict,
        autocommit: bool = True,
    ):
        """
        Args:
            settings (dict): Database connection settings.
            autocommit (bool, optional): Defaults to True.
        """

        self.is_autocommit = autocommit
        self.settings = settings

    def execute(self, *args, **kwargs): ...

    def executemany(self, *args, **kwargs): ...

    def fetchone(self): ...

    def fetchall(self): ...

    def commit(self): ...

    def rollback(self): ...

    def connect(self): ...

    def close(self): ...

    def is_alive(self) -> bool:
        """
        Check if the database connection is alive.
        Returns:
            bool: True if the connection is alive, False otherwise.
        """
        ...
