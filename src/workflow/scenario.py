import subprocess

from config import DDLStep
from db.postgres import PostgresResultsLoaded
from objects import ListOfQueries
from workflow.evaluator import QueryEvaluator
from utils import evaluate_sql


class Scenario:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
        self.sut_database = self.config.database

    def start_db(self):
        self.logger.info(f"Initializing {self.sut_database.__class__.__name__} DB")

        commit_hash = self.config.revision

        self.sut_database.change_version_and_compile(commit_hash)
        self.sut_database.stop_database()
        self.sut_database.destroy()
        self.sut_database.start_database()

        return self.get_commit_message(commit_hash)

    def get_commit_message(self, commit_hash):
        if commit_hash:
            output = str(subprocess.check_output(
                f"echo `git log -n 1 --pretty=format:%s {commit_hash}`",
                cwd=self.config.source_path,
                shell=True)).rstrip('\n')
            return f"{output} ({commit_hash})"
        else:
            return ""

    def stop_db(self):
        self.sut_database.stop_database()

    def evaluate(self):
        evaluator = QueryEvaluator(self.config)
        loader = PostgresResultsLoaded()

        commit_message = self.start_db()
        try:
            test_database = self.config.connection.database
            self.create_test_database(test_database)

            self.sut_database.establish_connection(test_database)

            loq = ListOfQueries(db_version=self.sut_database.connection.get_version(),
                                git_message=commit_message,
                                queries=evaluator.evaluate(self.sut_database.connection.conn,
                                                           self.config.with_optimizations))

            self.logger.info(f"Storing results to report/{self.config.output}")
            loader.store_queries_to_file(loq, self.config.output)
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            if self.config.clean_db:
                self.stop_db()

    def create_test_database(self, test_database):
        if DDLStep.DATABASE in self.config.ddls:
            self.sut_database.establish_connection("postgres")
            conn = self.sut_database.connection.conn
            try:
                with conn.cursor() as cur:
                    colocated = "" if self.config.ddl_prefix else " WITH COLOCATED = true"
                    evaluate_sql(cur, f'CREATE DATABASE {test_database}{colocated};')
            except Exception as e:
                self.logger.exception(f"Failed to create testing database {e}")
