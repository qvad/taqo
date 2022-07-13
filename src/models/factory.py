from config import Config
from models.simple import SimpleModel
from models.sql import SQLModel


def get_test_model():
    return SimpleModel() if Config().model == "simple" else SQLModel()
