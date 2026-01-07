from abc import ABC, abstractmethod


class Database(ABC):
    """
    Abstract base class for all database connections.
    Defines the minimum set of operations that every
    database implementation must provide.
    """

    @abstractmethod
    def close_connection(self):
        pass

    @abstractmethod
    def run_query(self, query: str):
        pass

    @abstractmethod
    def fetch_one(self, query: str):
        pass

    @abstractmethod
    def fetch_all(self, query: str):
        pass

    @abstractmethod
    def truncate_table(self, table_name: str):
        pass

