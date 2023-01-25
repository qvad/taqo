import subprocess

import psycopg2
from tqdm import tqdm

from config import DDLStep
from db.yugabyte import ENABLE_STATISTICS_HINT
from models.factory import get_test_model
from utils import evaluate_sql, calculate_avg_execution_time, get_md5


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
        loader = self.config.database.get_results_loader()

        commit_message = self.start_db()
        try:
            test_database = self.config.connection.database
            self.create_test_database(test_database)

            self.sut_database.establish_connection(test_database)

            loq = self.config.database.get_list_queries()
            loq.db_version = self.sut_database.connection.get_version()
            loq.model_queries, loq.queries = self.run_ddl_and_testing_queries(
                self.sut_database.connection.conn, self.config.with_optimizations)
            loq.git_message = commit_message

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

    def run_ddl_and_testing_queries(self,
                                    connection,
                                    evaluate_optimizations=False):
        queries = []
        model_queries = []
        try:
            model = get_test_model()
            created_tables, model_queries = model.create_tables(connection)
            queries = model.get_queries(created_tables)
        except Exception as e:
            self.logger.exception("Failed to evaluate DDL queries", e)
            exit(1)

        connection.autocommit = False
        self.evaluate_testing_queries(connection, queries, evaluate_optimizations)

        return model_queries, queries

    def evaluate_testing_queries(self, conn, queries, evaluate_optimizations):
        counter = 1
        for original_query in queries:
            with conn.cursor() as cur:
                for query in self.config.session_props:
                    evaluate_sql(cur, query)

                if self.config.enable_statistics:
                    self.logger.debug("Enable yb_enable_optimizer_statistics flag")

                    evaluate_sql(cur, ENABLE_STATISTICS_HINT)

                try:
                    evaluate_sql(cur,
                                 f"SET statement_timeout = '{self.config.test_query_timeout}s'")

                    short_query = original_query.query.replace('\n', '')[:40]
                    self.logger.info(
                        f"Evaluating query {short_query}... [{counter}/{len(queries)}]")

                    try:
                        evaluate_sql(cur, original_query.get_explain())
                        original_query.execution_plan = self.config.database.get_execution_plan(
                            '\n'.join(
                                str(item[0]) for item in cur.fetchall()))

                        conn.rollback()
                    except psycopg2.errors.QueryCanceled:
                        try:
                            evaluate_sql(cur, original_query.get_heuristic_explain())
                            original_query.execution_plan = self.config.database.get_execution_plan(
                                '\n'.join(
                                    str(item[0]) for item in cur.fetchall()))

                            conn.rollback()
                        except psycopg2.errors.QueryCanceled:
                            self.logger.error("Unable to get execution plan even w/o analyze")
                            original_query.execution_plan = self.config.database.get_execution_plan(
                                '')

                    calculate_avg_execution_time(cur, original_query,
                                                 num_retries=int(self.config.num_retries),
                                                 connection=conn)

                    if evaluate_optimizations and "dml" not in original_query.optimizer_tips.tags:
                        self.logger.debug("Evaluating optimizations...")
                        self.evaluate_optimizations(conn, cur, original_query)

                except psycopg2.Error as pe:
                    # do not raise exception
                    self.logger.exception(f"{original_query}\nFailed because of {pe}")
                except Exception as e:
                    self.logger.info(original_query)
                    raise e
                finally:
                    counter += 1

            conn.rollback()

    def evaluate_optimizations(self, connection, cur, original_query):
        # build all possible optimizations
        database = self.config.database
        list_of_optimizations = database.get_list_optimizations(original_query)

        self.logger.debug(f"{len(list_of_optimizations)} optimizations generated")
        progress_bar = tqdm(list_of_optimizations)
        num_skipped = 0
        min_execution_time = original_query.execution_time_ms if original_query.execution_time_ms > 0 else (
                    self.config.test_query_timeout * 1000)
        original_query.optimizations = []
        execution_plans_checked = set()

        for optimization in progress_bar:
            # in case of enable statistics enabled
            # we can get failure here and throw timeout
            original_query.optimizations.append(optimization)

            # set maximum execution time if this is first query,
            # or we are evaluating queries near best execution time
            if self.config.look_near_best_plan or len(original_query.optimizations) == 1:
                optimizer_query_timeout = \
                    (original_query.optimizer_tips and original_query.optimizer_tips.max_timeout) or \
                    f"{int(min_execution_time / 1000) + int(self.config.skip_timeout_delta)}s"

                self.logger.debug(f"Setting query timeout to {optimizer_query_timeout} seconds")

                evaluate_sql(cur, f"SET statement_timeout = '{optimizer_query_timeout}'")

            self.try_to_get_default_explain_hints(cur, optimization, original_query)

            try:
                evaluate_sql(cur, optimization.get_explain())
                optimization.execution_plan = database.get_execution_plan(
                    '\n'.join(
                        str(item[0]) for item in cur.fetchall()))

                connection.rollback()
            except psycopg2.errors.QueryCanceled as e:
                # failed by timeout - it's ok just skip optimization
                self.logger.debug(f"Getting execution plan failed with {e}")

                num_skipped += 1
                optimization.execution_time_ms = 0
                optimization.execution_plan = database.get_execution_plan("")
                continue

            exec_plan_md5 = get_md5(optimization.execution_plan.get_clean_plan())
            not_unique_plan = exec_plan_md5 in execution_plans_checked
            execution_plans_checked.add(exec_plan_md5)

            if not_unique_plan or not calculate_avg_execution_time(
                    cur,
                    optimization,
                    num_retries=int(self.config.num_retries),
                    connection=connection):
                num_skipped += 1

            # get new minimum execution time
            if optimization.execution_time_ms != 0 and \
                    optimization.execution_time_ms < min_execution_time:
                min_execution_time = optimization.execution_time_ms

            progress_bar.set_postfix(
                {'skipped': num_skipped, 'min_execution_time_ms': min_execution_time})

        return list_of_optimizations

    def try_to_get_default_explain_hints(self, cur, optimization, original_query):
        if not original_query.explain_hints:
            if self.config.enable_statistics or optimization.execution_plan is None:
                evaluate_sql(cur, optimization.get_heuristic_explain())

                execution_plan = self.config.database.get_execution_plan('\n'.join(
                    str(item[0]) for item in cur.fetchall()))
            else:
                execution_plan = optimization.execution_plan

            if original_query.compare_plans(execution_plan) and original_query.tips_looks_fair(
                    optimization):
                # store execution plan hints from optimization
                original_query.explain_hints = optimization.explain_hints
