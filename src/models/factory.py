from config import Config
from models.complex import ComplexModel
from models.sql import SQLModel, BasicOpsModel


def get_test_model():
    if Config().model == "complex":
        return ComplexModel()
    elif Config().model == "basic":
        return BasicOpsModel()
    else:
        return SQLModel()
