from config import ConnectionConfig
from database import Connection


class Postgres:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
        self.connection = None

    def establish_connection(self, database: str = "postgres"):
        config = ConnectionConfig(
            self.config.connection.host,
            self.config.connection.port,
            self.config.connection.username,
            self.config.connection.password,
            database,)
        self.connection = Connection(config)

        self.connection.connect()