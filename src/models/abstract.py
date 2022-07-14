import abc

from abc import ABC

from config import Config


class QTFModel(ABC):
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

    @abc.abstractmethod
    def create_tables(self, conn):
        pass

    @abc.abstractmethod
    def get_queries(self, tables):
        pass
