class Database:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
        self.connection = None

    def change_version_and_compile(self, revision_or_path: str = None):
        pass

    def destroy(self):
        pass

    def start_database(self):
        pass

    def stop_database(self):
        pass

    def call_upgrade_ysql(self):
        pass

    def get_list_optimizations(self, original_query):
        pass

    def get_execution_plan(self, execution_plan: str):
        pass

    def get_results_loader(self):
        pass
