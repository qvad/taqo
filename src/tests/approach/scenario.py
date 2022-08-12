from database import ENABLE_STATISTICS_HINT
from models.factory import get_test_model
from tests.abstract import AbstractTest
from tests.approach.report import ApproachReport
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql


class ApproachTest(AbstractTest):

    def __init__(self):
        super().__init__()
        self.report = ApproachReport()

    def evaluate_queries_for_version(self, conn, queries):
        version_queries = []
        with conn.cursor() as cur:
            counter = 1
            for query in queries:
                try:
                    self.logger.info(
                        f"Evaluating query {query.query[:40]}... [{counter}/{len(queries)}]")
                    evaluate_sql(cur, query.get_explain_analyze())
                    query.execution_plan = '\n'.join(str(item[0]) for item in cur.fetchall())
                    query.optimizer_score = \
                        get_optimizer_score_from_plan(query.execution_plan)

                    calculate_avg_execution_time(cur, query, query_str=query.get_explain_analyze(),
                                                 num_retries=int(self.config.num_retries))

                    version_queries.append(query)
                except Exception as e:
                    raise e
                finally:
                    counter += 1

        return version_queries

    def evaluate(self):
        self.start_db()

        conn = None

        try:
            self.yugabyte.establish_connection()
            conn = self.yugabyte.connection.conn

            with conn.cursor() as cur:
                evaluate_sql(cur, 'SELECT VERSION();')
                first_version = cur.fetchone()[0]
                self.logger.info(f"Running regression test against {first_version}")

            # evaluate original query
            model = get_test_model()
            created_tables = model.create_tables(conn, skip_analyze=True)
            queries_default = model.get_queries(created_tables)

            queries_default_plans = self.evaluate_queries_for_version(conn, queries_default)

            self.logger.info("Evaluating with ANALYZE")

            with conn.cursor() as cur:
                for table in created_tables:
                    evaluate_sql(cur, f'ANALYZE {table.name};')

            queries_analyze = model.get_queries(created_tables)
            queries_analyze_plans = self.evaluate_queries_for_version(conn, queries_analyze)

            self.logger.info("Evaluating with statistics and ANALYZE")
            with conn.cursor() as cur:
                evaluate_sql(cur, ENABLE_STATISTICS_HINT)

            queries_all = model.get_queries(created_tables)
            queries_all_plans = self.evaluate_queries_for_version(conn, queries_all)

            for query_id in range(len(queries_default_plans)):
                self.report.add_query(queries_default_plans[query_id], queries_analyze_plans[query_id], queries_all_plans[query_id])
        finally:
            # publish current report
            self.report.build_report()
            self.report.publish_report("analyze")

            # close connection
            conn.close()

            # stop yugabyte
            self.stop_db()
