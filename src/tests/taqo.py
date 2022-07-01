import time
import psycopg2

from tqdm import tqdm

from src.config import Config
from src.database import ListOfOptimizations
from src.models.factory import get_test_model
from src.report import Report
from src.utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql

ENABLE_PLAN_HINTING = "SET pg_hint_plan.enable_hint = ON;"
ENABLE_STATISTICS_HINT = "SET yb_enable_optimizer_statistics = true;"


def evaluate_taqo():
    conn = None
    report = None
    config = Config()

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
            report = Report(cur.fetchone()[0])

        # evaluate original query
        model = get_test_model()
        model.create_tables(conn)
        queries = model.get_queries()

        if config.num_queries:
            queries = queries[:int(config.num_queries)]

        time.sleep(10)

        with conn.cursor() as cur:
            counter = 1

            evaluate_sql(cur, ENABLE_PLAN_HINTING)
            if config.enable_statistics:
                if Config().verbose:
                    print("Enable yb_enable_optimizer_statistics flag")

                evaluate_sql(cur, ENABLE_STATISTICS_HINT)

            for original_query in queries:
                try:
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
                        original_query.optimizer_tips.max_timeout or \
                        f"{int(original_query.execution_time_ms / 1000) + int(config.skip_timeout_delta)}s"

                    if Config().verbose:
                        print(f"Setting query timeout to {optimizer_query_timeout} seconds")

                    evaluate_sql(cur, f"SET statement_timeout = '{optimizer_query_timeout}'")

                    # build all possible optimizations
                    list_of_optimizations = ListOfOptimizations(
                        original_query) \
                        .get_all_optimizations(int(config.max_optimizations))
                    for optimization in tqdm(list_of_optimizations):
                        evaluate_sql(cur, optimization.get_explain())
                        optimization.execution_plan = '\n'.join(
                            str(item[0]) for item in cur.fetchall())

                        optimization.optimizer_score = get_optimizer_score_from_plan(
                            optimization.execution_plan)

                        calculate_avg_execution_time(cur, optimization, int(config.num_retries))

                    best_optimization = original_query
                    for optimization in list_of_optimizations:
                        if best_optimization.execution_time_ms > optimization.execution_time_ms != 0:
                            best_optimization = optimization

                    report.add_taqo_query(query=original_query,
                                          best_optimization=best_optimization,
                                          optimizations=list_of_optimizations)
                except Exception as e:
                    print(original_query)
                    raise e
                finally:
                    counter += 1
    finally:
        # publish current report
        report.publish_report("taqo")

        # close connection
        conn.close()
