from src.config import Config
from src.models.simple import SimpleModel
from src.models.sql import SQLModel


def get_test_model():
    if Config().model == "simple":
        return SimpleModel()
    else:
        return SQLModel()
