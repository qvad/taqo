import abc

from abc import ABC


class QTFModel(ABC):
    @abc.abstractmethod
    def create_tables(self, conn):
        pass

    @abc.abstractmethod
    def get_queries(self, tables):
        pass
