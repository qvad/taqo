class Database:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
        self.connection = None

    def establish_connection(self, database: str):
        pass

    def get_list_queries(self):
        pass

    def change_version_and_compile(self, revision_or_path: str = None):
        pass

    def create_test_database(self):
        pass

    def prepare_query_execution(self, cur):
        pass

    def set_query_timeout(self, cur, timeout):
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