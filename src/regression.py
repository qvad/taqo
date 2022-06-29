import psycopg2

from model import create_tables, create_queries
from report import Report
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time


def evaluate_regression(args):
    # todo implement
    print("Not implemented")
    exit(1)

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

        with conn.cursor() as cur:
            counter = 1
            for original_query in queries:
                try:
                    print(
                        f"Evaluating query {original_query.query[:40]}... [{counter}/{len(queries)}]")
                    cur.execute(original_query.get_explain())
                    original_query.execution_plan = '\n'.join(
                        str(item[0]) for item in cur.fetchall())
                    original_query.optimizer_score = get_optimizer_score_from_plan(
                        original_query.execution_plan)

                    calculate_avg_execution_time(cur, original_query, int(args.num_retries))
                except Exception as e:
                    raise e
    finally:
        conn.close()
