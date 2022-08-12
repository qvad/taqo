import abc
from abc import ABC
from enum import Enum

from config import Config


class QueryJoins(Enum):
    INNER = "INNER"
    RIGHT_OUTER = "RIGHT OUTER"
    LEFT_OUTER = "LEFT OUTER"
    FULL_OUTER = "FULL"


class QTFModel(ABC):
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

    def generate_data(self):
        pass

    @abc.abstractmethod
    def create_tables(self, conn, skip_analyze=False, db_prefix=None):
        pass

    @abc.abstractmethod
    def get_queries(self, tables):
        pass
