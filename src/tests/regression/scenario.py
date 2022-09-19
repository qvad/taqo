from database import ListOfQueries, get_queries_from_previous_result, store_queries_to_file, \
    ExecutionPlan
from models.factory import get_test_model
from tests.abstract import AbstractTest
from tests.regression.report import RegressionReport
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql


class RegressionTest(AbstractTest):

    def __init__(self):
        super().__init__()
        self.report = RegressionReport()

    def evaluate_queries_for_version(self, conn, queries, version_props):
        version_queries = ListOfQueries()
        with conn.cursor() as cur:
            session_props = version_props or self.config.session_props

            for query in session_props:
                evaluate_sql(cur, query)

            counter = 1
            for first_version_query in queries:
                try:
                    self.logger.info(
                        f"Evaluating query {first_version_query.query[:40]}... [{counter}/{len(queries)}]")
                    evaluate_sql(cur, first_version_query.get_explain())
                    first_version_query.execution_plan = ExecutionPlan('\n'.join(
                        str(item[0]) for item in cur.fetchall()))
                    first_version_query.optimizer_score = get_optimizer_score_from_plan(
                        first_version_query.execution_plan)

                    calculate_avg_execution_time(cur, first_version_query,
                                                 num_retries=int(self.config.num_retries))

                    version_queries.append(first_version_query)
                except Exception as e:
                    raise e
                finally:
                    counter += 1

        return version_queries

    def switch_version(self):
        self.logger.info(f"Switching Yugabyte version to {self.config.revisions_or_paths[1]}")

        self.yugabyte.stop_database()
        self.yugabyte.change_version_and_compile(self.config.revisions_or_paths[1])
        # todo is this correct upgrade path?
        self.yugabyte.start_database()
        self.yugabyte.call_upgrade_ysql()

        return self.get_commit_message(self.config.revisions_or_paths[1])

    def evaluate(self):
        first_commit_message = self.start_db()

        conn = None

        try:
            self.yugabyte.establish_connection()
            conn = self.yugabyte.connection.conn

            model = get_test_model()
            created_tables, model_queries = model.create_tables(conn)
            self.report.report_model(model_queries)
            if not self.config.previous_results_path:
                with conn.cursor() as cur:
                    evaluate_sql(cur, 'SELECT VERSION();')
                    first_version = cur.fetchone()[0]
                    self.logger.info(f"Running regression test against {first_version}")

                first_queries = model.get_queries(created_tables)
                first_version_queries = self.evaluate_queries_for_version(
                    conn, first_queries, self.config.session_props_v1)
                first_version_queries.db_version = first_version
                first_version_queries.git_message = first_commit_message

                conn.close()
            else:
                first_version_queries = get_queries_from_previous_result(
                    self.config.previous_results_path)

                # reconnect
                conn = self.evaluate_and_compare_with_second_version(created_tables,
                                                                     first_version_queries,
                                                                     first_commit_message,
                                                                     model)

            if len(self.config.revisions_or_paths) == 2 and self.config.revisions_or_paths[1]:
                second_commit_message = self.switch_version()

                # reconnect
                conn = self.evaluate_and_compare_with_second_version(created_tables,
                                                                     first_version_queries,
                                                                     second_commit_message,
                                                                     model)
            else:
                store_queries_to_file(first_version_queries)
        finally:
            # publish current report
            self.report.build_report()
            self.report.publish_report("regression")

            # close connection
            conn.close()

            # stop yugabyte
            self.stop_db()

    def evaluate_and_compare_with_second_version(self,
                                                 created_tables,
                                                 first_version_queries,
                                                 second_commit_message,
                                                 model):
        self.logger.info("Reconnecting to DB after upgrade")
        self.yugabyte.establish_connection()
        conn = self.yugabyte.connection.conn

        second_queries = model.get_queries(created_tables)
        with conn.cursor() as cur:
            evaluate_sql(cur, 'SELECT VERSION();')
            second_version = cur.fetchone()[0]
            self.logger.info(f"Running regression test against {second_version}")

        if first_version_queries.git_message and second_commit_message:
            self.report.define_version(first_version_queries.git_message, second_commit_message)
        else:
            self.report.define_version(first_version_queries.db_version, second_version)

        second_version_queries = self.evaluate_queries_for_version(conn, second_queries, self.config.session_props_v2)

        for first_version_query, second_version_query in zip(first_version_queries.queries,
                                                             second_version_queries.queries):
            self.report.add_query(first_version_query, second_version_query)

        return conn
