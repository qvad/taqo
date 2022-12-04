

class Database:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
        self.connection = None

    def change_version_and_compile(self, revision_or_path=None):
        pass

    def destroy(self):
        pass

    def start_database(self):
        pass

    def stop_database(self):
        pass

    def call_upgrade_ysql(self):
        pass