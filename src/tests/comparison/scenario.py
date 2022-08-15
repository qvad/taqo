from db.postgres import Postgres
from models.factory import get_test_model
from tests.abstract import AbstractTest
from tests.comparison.report import ComparisonReport
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql


class ComparisonTest(AbstractTest):

    def __init__(self):
        super().__init__()
        self.report = ComparisonReport()
        self.postgres = Postgres(self.config)

    def evaluate_queries_for_version(self, conn, queries):
        version_queries = []
        with conn.cursor() as cur:
            counter = 1
            for first_version_query in queries:
                try:
                    self.logger.info(
                        f"Evaluating query {first_version_query.query[:40]}... [{counter}/{len(queries)}]")
                    evaluate_sql(cur, first_version_query.get_explain())
                    first_version_query.execution_plan = '\n'.join(
                        str(item[0]) for item in cur.fetchall())
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

    def evaluate(self):
        self.start_db()

        conn = None

        try:
            self.logger.info("Running queries against yugabyte")
            self.yugabyte.establish_connection()
            conn = self.yugabyte.connection.conn

            with conn.cursor() as cur:
                evaluate_sql(cur, 'SELECT VERSION();')
                yb_version = cur.fetchone()[0]
                self.logger.info(f"Running comparison test against {yb_version}")

            # evaluate original query
            model = get_test_model()
            created_tables = model.create_tables(conn)
            queries = model.get_queries(created_tables)

            yb_version_queries = self.evaluate_queries_for_version(conn, queries)

            conn.close()

            # reconnect
            self.logger.info("Running queries against postgres")
            self.postgres.establish_connection()
            conn = self.postgres.connection.conn
            pg_created_tables = model.create_tables(conn, "postgres")
            pg_queries = model.get_queries(pg_created_tables)

            with conn.cursor() as cur:
                evaluate_sql(cur, 'SELECT VERSION();')
                second_version = cur.fetchone()[0]
                self.logger.info(f"Running comparison test against {second_version}")

            self.report.define_version(yb_version, second_version)

            postgres_version_queries = self.evaluate_queries_for_version(conn, pg_queries)

            for yb_version_query, pg_version_query in zip(yb_version_queries,
                                                          postgres_version_queries):
                self.report.add_query(yb_version_query, pg_version_query)
        finally:
            # publish current report
            self.report.build_report()
            self.report.publish_report("comparison")

            # close connection
            conn.close()

            # stop yugabyte
            self.stop_db()
