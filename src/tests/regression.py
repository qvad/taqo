import psycopg2

from config import Config
from models.factory import get_test_model
from report import RegressionReport
from utils import get_optimizer_score_from_plan, calculate_avg_execution_time, evaluate_sql
from yugabyte import factory


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
            finally:
                counter += 1

    return version_queries


def evaluate_regression():
    config = Config()

    yugabyte = start_yugabyte(config)

    conn = None
    report = RegressionReport()

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
            first_version = cur.fetchone()[0]

        # evaluate original query
        model = get_test_model()
        created_tables = model.create_tables(conn)
        queries = model.get_queries(created_tables)

        first_version_queries = evaluate_queries_for_version(conn, queries)

        conn.close()

        yugabyte.stop_node()
        yugabyte.change_version_and_compile(config.revisions_or_paths[1])
        # tod is this correct upgrade path?
        yugabyte.start_node()
        yugabyte.call_upgrade_ysql()

        # reconnect
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.username,
            password=config.password)
        conn.autocommit = True

        with conn.cursor() as cur:
            evaluate_sql(cur, 'SELECT VERSION();')
            second_version = cur.fetchone()[0]

        report.define_versions(first_version, second_version)

        second_version_queries = evaluate_queries_for_version(conn, queries)

        for first_version_query, second_version_query in zip(first_version_queries,
                                                             second_version_queries):
            report.add_query(first_version_query, second_version_query)
    finally:
        # publish current report
        report.build_report()
        report.publish_report("regression")

        # close connection
        conn.close()

        # stop yugabyte
        yugabyte.stop_node()


def start_yugabyte(config):
    yugabyte = factory(config)
    yugabyte.change_version_and_compile(config.revisions_or_paths[0])
    yugabyte.stop_node()
    yugabyte.destroy()
    yugabyte.start_node()

    return yugabyte
