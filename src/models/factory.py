from config import Config
from models.complex import ComplexModel
from models.simple import SimpleModel
from models.sql import SQLModel, BasicOpsModel


def get_test_model():
    if Config().model == "simple":
        return SimpleModel()
    elif Config().model == "complex":
        return ComplexModel()
    elif Config().model == "basic":
        return BasicOpsModel()
    else:
        return SQLModel()
