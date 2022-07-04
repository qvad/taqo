import psycopg2

from src.config import Config
from src.models.factory import get_test_model
from src.report import Report
from src.utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql


def evaluate_queries_for_version(conn, queries):
    version_queries = []
    with conn.cursor() as cur:
        counter = 1
        for first_version_query in queries:
            try:
                print(
                    f"Evaluating query {first_version_query.query[:40]}... [{counter}/{len(queries)}]")
                evaluate_sql(cur, first_version_query.get_explain())
                first_version_query.execution_plan = '\n'.join(
                    str(item[0]) for item in cur.fetchall())
                first_version_query.optimizer_score = get_optimizer_score_from_plan(
                    first_version_query.execution_plan)

                calculate_avg_execution_time(cur, first_version_query, int(Config().num_retries))

                version_queries.append(first_version_query)
            except Exception as e:
                raise e

    return version_queries


def evaluate_regression():
    # todo implement
    print("Not implemented")
    exit(1)

    conn = None
    report = Report()
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
            report.version = cur.fetchone()[0]

        # evaluate original query
        model = get_test_model()
        created_tables = model.create_tables(conn)
        queries = model.get_queries(created_tables)

        first_version_queries = evaluate_queries_for_version(conn, queries)

        # todo magic upgrade happens here

        second_version_queries = evaluate_queries_for_version(conn, queries)

        for first_version_query, second_version_query in zip(first_version_queries,
                                                             second_version_queries):
            report.add_regression_query(first_version_query, second_version_query)
    finally:
        # publish current report
        report.publish_report("regression")

        # close connection
        conn.close()
