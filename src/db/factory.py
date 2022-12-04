from db.postgres import Postgres
from db.yugabyte import yb_db_factory


def create_database(db, config):
    database = None
    if db == "postgres":
        database = Postgres(config)
    elif db == "yugabyte":
        database = yb_db_factory(config)

    return database
