import time
import psycopg2

from tqdm import tqdm
from database import ListOfOptimizations
from model import create_queries, create_tables
from report import Report
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time

EXPLAIN = "EXPLAIN "
ENABLE_HINT = "SET pg_hint_plan.enable_hint = ON;"


def evaluate_taqo(args):
    conn = None
    report = Report()

    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.username,
            password=args.password)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute('SELECT VERSION();')
            version = cur.fetchone()[0]
            print(version)

        # evaluate original query
        create_tables(conn)
        queries = create_queries()

        if args.num_queries:
            queries = queries[:int(args.num_queries)]

        time.sleep(10)

        with conn.cursor() as cur:
            counter = 1
            for original_query in queries:
                try:
                    print(f"Evaluating query {original_query.query[:40]}... [{counter}/{len(queries)}]")
                    cur.execute(original_query.get_explain())
                    original_query.execution_plan = '\n'.join(str(item[0]) for item in cur.fetchall())
                    original_query.optimizer_score = get_optimizer_score_from_plan(
                        original_query.execution_plan)

                    calculate_avg_execution_time(cur, original_query, args.num_retries)

                    # set maximum execution time
                    print(f"Setting query timeout to {int(original_query.execution_time_ms / 1000) + int(args.skip_timeout)} seconds")
                    cur.execute(
                        f"SET statement_timeout = '{int(original_query.execution_time_ms / 1000) + int(args.skip_timeout)}s'")

                    # build all possible optimizations
                    list_of_optimizations = ListOfOptimizations(original_query.query,
                                                                original_query.tables).get_all_optimizations()

                    if args.num_optimizations:
                        list_of_optimizations = list_of_optimizations[:int(args.num_optimizations)]

                    for optimization in tqdm(list_of_optimizations):
                        cur.execute(ENABLE_HINT)
                        cur.execute(optimization.get_explain())
                        optimization.execution_plan = '\n'.join(str(item[0]) for item in cur.fetchall())

                        optimization.optimizer_score = get_optimizer_score_from_plan(
                            optimization.execution_plan)

                        calculate_avg_execution_time(cur, optimization, args.num_retries)

                    best_optimization = original_query
                    for optimization in list_of_optimizations:
                        if best_optimization.execution_time_ms > optimization.execution_time_ms != 0:
                            best_optimization = optimization

                    report.add_query(query=original_query,
                                     best_optimization=best_optimization,
                                     optimizations=list_of_optimizations)
                except Exception as e:
                    print(original_query)
                    raise e
                finally:
                    counter += 1
    finally:
        # publish current report
        report.publish_report()
        # close connection
        conn.close()
