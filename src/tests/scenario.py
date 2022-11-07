import subprocess

from database import store_queries_to_file, ListOfQueries
from db.yugabyte import factory
from tests.evaluator import QueryEvaluator
from utils import evaluate_sql


class Scenario():
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger

    def start_db(self):
        self.logger.info("Starting Yugabyte DB")

        self.yugabyte = factory(self.config)

        commit_hash = self.config.revision

        self.yugabyte.change_version_and_compile(commit_hash)
        self.yugabyte.stop_database()
        self.yugabyte.destroy()
        self.yugabyte.start_database()

        return self.get_commit_message(commit_hash)

    def get_commit_message(self, commit_hash):
        output = str(subprocess.check_output(
            f"echo `git log -n 1 --pretty=format:%s {commit_hash}`",
            cwd=self.config.yugabyte_code_path,
            shell=True)).rstrip('\n')
        return f"{output} ({commit_hash})" if commit_hash else ""

    def stop_db(self):
        self.yugabyte.stop_database()

    def evaluate(self):
        evaluator = QueryEvaluator(self.config)

        commit_message = self.start_db()
        try:
            test_database = self.config.yugabyte.database
            self.create_test_database(test_database)

            self.yugabyte.establish_connection(test_database)

            loq = ListOfQueries(db_version=self.yugabyte.connection.get_version(),
                                git_message=commit_message,
                                queries=evaluator.evaluate(self.yugabyte.connection.conn,
                                                           self.config.with_optimizations))

            store_queries_to_file(loq, self.config.output)
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            self.stop_db()

    def create_test_database(self, test_database):
        self.yugabyte.establish_connection("postgres")
        conn = self.yugabyte.connection.conn
        try:
            with conn.cursor() as cur:
                colocated = " WITH COLOCATED = true" if self.config.ddl_prefix is not None else ""
                evaluate_sql(cur, f'CREATE DATABASE {test_database} {colocated};')
        except Exception as e:
            self.logger.exception(f"Failed to create testing database {e}")
