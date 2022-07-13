from config import Config
from models.simple import SimpleModel
from models.sql import SQLModel


def get_test_model():
    if Config().model == "simple":
        return SimpleModel()
    else:
        return SQLModel()
