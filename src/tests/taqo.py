import time

import psycopg2
from tqdm import tqdm

from config import Config
from database import ListOfOptimizations, ENABLE_PLAN_HINTING, ENABLE_STATISTICS_HINT
from models.factory import get_test_model
from report import TaqoReport
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql
from yugabyte import factory


def evaluate_taqo():
    config = Config()

    yugabyte = factory(config)
    yugabyte.change_version_and_compile(config.revisions_or_paths[0])
    yugabyte.stop_node()
    yugabyte.destroy()
    yugabyte.start_node()

    conn = None
    report = None

    try:
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.username,
            password=config.password)
        conn.autocommit = True

        with conn.cursor() as cur:
            evaluate_sql(cur, 'SELECT VERSION();')
            report = TaqoReport(cur.fetchone()[0])

        # evaluate original query
        model = get_test_model()
        created_tables = model.create_tables(conn)
        queries = model.get_queries(created_tables)

        if config.num_queries:
            queries = queries[:int(config.num_queries)]

        time.sleep(10)

        with conn.cursor() as cur:
            counter = 1

            evaluate_sql(cur, ENABLE_PLAN_HINTING)
            if config.enable_statistics:
                if config.verbose:
                    print("Enable yb_enable_optimizer_statistics flag")

                evaluate_sql(cur, ENABLE_STATISTICS_HINT)

            for original_query in queries:
                try:
                    evaluate_sql(cur, "SET statement_timeout = '1200s'")

                    short_query = original_query.query.replace('\n', '')[:40]
                    print(f"Evaluating query {short_query}... [{counter}/{len(queries)}]")

                    evaluate_sql(cur, original_query.get_explain())
                    original_query.execution_plan = '\n'.join(
                        str(item[0]) for item in cur.fetchall())
                    original_query.optimizer_score = get_optimizer_score_from_plan(
                        original_query.execution_plan)

                    calculate_avg_execution_time(cur, original_query, int(config.num_retries))

                    # set maximum execution time
                    optimizer_query_timeout = \
                        (
                                original_query.optimizer_tips and original_query.optimizer_tips.max_timeout) or \
                        f"{int(original_query.execution_time_ms / 1000) + int(config.skip_timeout_delta)}s"

                    if config.verbose:
                        print(f"Setting query timeout to {optimizer_query_timeout} seconds")

                    evaluate_sql(cur, f"SET statement_timeout = '{optimizer_query_timeout}'")

                    evaluate_optimizations(config, cur, original_query)

                    report.add_query(original_query)
                except Exception as e:
                    print(original_query)
                    raise e
                finally:
                    counter += 1
    finally:
        # publish current report
        report.build_report()
        report.publish_report("taqo")

        # close connection
        conn.close()

        # stop yugabyte
        yugabyte.stop_node()


def evaluate_optimizations(config, cur, original_query):
    # build all possible optimizations
    list_of_optimizations = ListOfOptimizations(
        original_query) \
        .get_all_optimizations(int(config.max_optimizations))
    progress_bar = tqdm(list_of_optimizations)
    num_skipped = 0
    original_query.optimizations = []
    for optimization in progress_bar:
        # in case of enable statistics enabled
        # we can get failure here and throw timeout
        original_query.optimizations.append(optimization)

        try:
            evaluate_sql(cur, optimization.get_explain())
        except psycopg2.errors.QueryCanceled as e:
            # failed by timeout - it's ok just skip optimization
            if config.verbose:
                print(f"Getting execution plan failed with {e}")

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
                                            int(config.num_retries)):
            num_skipped += 1

        progress_bar.set_postfix({'skipped': num_skipped})
    return list_of_optimizations
