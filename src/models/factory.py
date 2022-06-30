from src.models.simple import SimpleModel
from src.models.tpch import TPCHModel


def get_test_model(args):
    if args.model == "simple":
        return SimpleModel()
    elif args.model == "tpch":
        return TPCHModel()
    else:
        raise AttributeError(f'Unknown model {args.model}')
