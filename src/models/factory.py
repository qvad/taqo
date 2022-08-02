from config import Config
from models.complex import ComplexModel
from models.simple import SimpleModel
from models.sql import SQLModel


def get_test_model():
    if Config().model == "simple":
        return SimpleModel()
    elif Config().model == "complex":
        return ComplexModel()
    else:
        return SQLModel()
