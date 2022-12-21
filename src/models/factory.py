from config import Config
from models.sql import SQLModel, BasicOpsModel


def get_test_model():
    return BasicOpsModel() if Config().model == "basic" else SQLModel()
