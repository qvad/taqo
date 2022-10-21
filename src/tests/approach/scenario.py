import logging

from database import ENABLE_STATISTICS_HINT, ListOfQueries, ExecutionPlan
from models.factory import get_test_model
from tests.abstract import AbstractTest
from tests.approach.report import ApproachReport
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql


class ApproachTest(AbstractTest):

    def __init__(self):
        super().__init__()
        self.report = ApproachReport()

    def evaluate_queries_for_version(self, conn, queries, explain_with_analyze):
        version_queries = ListOfQueries()
        with conn.cursor() as cur:
            counter = 1
            for query in queries:
                try:
                    self.logger.info(
                        f"Evaluating query {query.query[:40]}... [{counter}/{len(queries)}]")
                    query_explain = query.get_explain_analyze() if explain_with_analyze else query.get_heuristic_explain()
                    evaluate_sql(cur, query_explain)
                    query.execution_plan = ExecutionPlan(
                        '\n'.join(str(item[0]) for item in cur.fetchall()))
                    query.optimizer_score = \
                        get_optimizer_score_from_plan(query.execution_plan)

                    query_executed = query.get_explain_analyze() if explain_with_analyze else query.get_heuristic_explain()
                    calculate_avg_execution_time(cur, query, query_str=query_executed,
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
            created_tables, model_queries = model.create_tables(conn, skip_analyze=True)
            queries_default = model.get_queries(created_tables)
            queries_default_with_analyze = model.get_queries(created_tables)

            self.report.report_model(model_queries)

            queries_default_plans = self.evaluate_queries_for_version(conn, queries_default, False)
            queries_default_plans_with_analyze = self.evaluate_queries_for_version(
                conn,
                queries_default_with_analyze,
                True)

            self.logger.info("Evaluating with ANALYZE")

            try:
                with conn.cursor() as cur:
                    for table in created_tables:
                        evaluate_sql(cur, f'ANALYZE {table.name};')
            except Exception as e:
                self.logger.exception("Evaluating with statistics and ANALYZE", e)

            queries_analyze = model.get_queries(created_tables)
            queries_analyze_with_analyze = model.get_queries(created_tables)

            queries_analyze_plans = self.evaluate_queries_for_version(conn, queries_analyze, False)
            queries_analyze_plans_with_analyze = self.evaluate_queries_for_version(
                conn,
                queries_analyze_with_analyze,
                True)

            try:
                self.logger.info("Evaluating with statistics and ANALYZE")
                with conn.cursor() as cur:
                    evaluate_sql(cur, ENABLE_STATISTICS_HINT)
            except Exception as e:
                self.logger.exception("Evaluating with statistics and ANALYZE", e)

            queries_all = model.get_queries(created_tables)
            queries_all_with_analyze = model.get_queries(created_tables)

            queries_all_plans = self.evaluate_queries_for_version(conn, queries_all, False)
            queries_all_plans_with_analyze = self.evaluate_queries_for_version(
                conn,
                queries_all_with_analyze,
                True)

            for query_id in range(len(queries_default_plans.queries)):
                self.report.add_query(
                    queries_default_plans.queries[query_id],
                    queries_default_plans_with_analyze.queries[query_id],
                    queries_analyze_plans.queries[query_id],
                    queries_analyze_plans_with_analyze.queries[query_id],
                    queries_all_plans.queries[query_id],
                    queries_all_plans_with_analyze.queries[query_id]
                )
        finally:
            # publish current report
            self.report.build_report()
            self.report.publish_report("analyze")

            # close connection
            conn.close()

            # stop yugabyte
            self.stop_db()
