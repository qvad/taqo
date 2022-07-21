from database import Connection


class Postgres:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
        self.connection = None

    def establish_connection(self):
        self.connection = Connection(self.config.postgres)

        self.connection.connect()