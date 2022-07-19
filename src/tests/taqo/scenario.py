import time

import psycopg2
from tqdm import tqdm

from database import ListOfOptimizations, ENABLE_PLAN_HINTING, ENABLE_STATISTICS_HINT
from models.factory import get_test_model
from tests.taqo.report import TaqoReport
from tests.abstract import AbstractTest
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql


class TaqoTest(AbstractTest):
    def __init__(self):
        super().__init__()
        self.report = TaqoReport()

    def evaluate(self):
        self.start_db()

        conn = None

        try:
            conn = self.connect_to_db()

            with conn.cursor() as cur:
                evaluate_sql(cur, 'SELECT VERSION();')
                self.report.define_version(cur.fetchone()[0])

            # evaluate original query
            model = get_test_model()
            created_tables = model.create_tables(conn)
            queries = model.get_queries(created_tables)

            time.sleep(10)

            with conn.cursor() as cur:
                counter = 1

                evaluate_sql(cur, ENABLE_PLAN_HINTING)
                if self.config.enable_statistics:
                    self.logger.debug("Enable yb_enable_optimizer_statistics flag")

                    evaluate_sql(cur, ENABLE_STATISTICS_HINT)

                for original_query in queries:
                    try:
                        evaluate_sql(cur, "SET statement_timeout = '1200s'")

                        short_query = original_query.query.replace('\n', '')[:40]
                        self.logger.info(f"Evaluating query {short_query}... [{counter}/{len(queries)}]")

                        evaluate_sql(cur, original_query.get_explain())
                        original_query.execution_plan = '\n'.join(
                            str(item[0]) for item in cur.fetchall())
                        original_query.optimizer_score = get_optimizer_score_from_plan(
                            original_query.execution_plan)

                        calculate_avg_execution_time(cur, original_query,
                                                     int(self.config.num_retries))

                        self.evaluate_optimizations(cur, original_query)

                        self.report.add_query(original_query)
                    except Exception as e:
                        self.logger.info(original_query)
                        raise e
                    finally:
                        counter += 1
        finally:
            # publish current report
            self.report.build_report()
            self.report.publish_report("taqo")

            # close connection
            conn.close()

            # stop yugabyte
            self.stop_db()

    def evaluate_optimizations(self, cur, original_query):
        # build all possible optimizations
        list_of_optimizations = ListOfOptimizations(
            original_query) \
            .get_all_optimizations(int(self.config.max_optimizations))
        progress_bar = tqdm(list_of_optimizations)
        num_skipped = 0
        min_execution_time = original_query.execution_time_ms
        original_query.optimizations = []
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

            try:
                evaluate_sql(cur, optimization.get_explain())
            except psycopg2.errors.QueryCanceled as e:
                # failed by timeout - it's ok just skip optimization
                self.logger.debug(f"Getting execution plan failed with {e}")

                num_skipped += 1
                optimization.execution_time_ms = 0
                optimization.execution_plan = ""
                optimization.optimizer_score = 0
                continue

            optimization.execution_plan = '\n'.join(
                str(item[0]) for item in cur.fetchall())

            optimization.optimizer_score = get_optimizer_score_from_plan(
                optimization.execution_plan)

            if not calculate_avg_execution_time(cur, optimization,
                                                int(self.config.num_retries)):
                num_skipped += 1

            # get new minimum execution time
            if optimization.execution_time_ms != 0 and \
                    optimization.execution_time_ms < min_execution_time:
                min_execution_time = optimization.execution_time_ms

            progress_bar.set_postfix({'skipped': num_skipped})
        return list_of_optimizations
