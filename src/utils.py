import re
import time

import psycopg2
from sql_metadata import Parser

from config import Config

EXPLAIN = "EXPLAIN "
EXPLAIN_ANALYZE = "EXPLAIN ANALYZE "


def current_milli_time():
    return round(time.time() * 1000)


def get_optimizer_score_from_plan(execution_plan):
    matches = re.finditer(r"\s\(cost=.*\.\.(\d+\.\d+)\s", execution_plan, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
        return float(match.groups()[0])


def calculate_avg_execution_time(cur, query, num_retries):
    config = Config()

    sum_execution_times = 0
    actual_evaluations = 0

    # run at least one iteration
    num_retries = max(num_retries, 2)

    for _ in range(num_retries):
        # noinspection PyUnresolvedReferences
        try:
            start_time = current_milli_time()
            evaluate_sql(cur, query.get_query())
            sum_execution_times += current_milli_time() - start_time
        except psycopg2.errors.QueryCanceled as qc:
            # failed by timeout - it's ok just skip optimization
            query.execution_time_ms = 0
            config.logger.debug(
                f"Skipping optimization due to timeout limitation: {query.get_query()}")
            return False
        except psycopg2.errors.DatabaseError as ie:
            # Some serious problem occured - probably an issue
            query.execution_time_ms = 0
            config.logger.error(f"INTERNAL ERROR {ie}\nSQL query:{query.get_query()}")
            return False
        # todo add exception when wrong hints used?
        finally:
            actual_evaluations += 1

    query.execution_time_ms = sum_execution_times / actual_evaluations

    return True


def get_alias_table_names(sql_str, table_names):
    parser = Parser(sql_str)

    # todo some workarounds for sql_metadata package issues
    # 1. 'where' may occur in table aliases
    tables = parser.tables
    aliases = parser.tables_aliases

    result_tables = {alias: table_name for alias, table_name in aliases.items()
                     if alias not in ['where', 'group by', 'from'] and table_name in table_names}

    for table in tables:
        if table not in result_tables.keys():
            result_tables[table] = table

    return result_tables


def get_explain_clause():
    return EXPLAIN_ANALYZE if Config().enable_statistics else EXPLAIN


def evaluate_sql(cur, sql):
    config = Config()

    config.logger.debug(
        sql.replace("\n", "")[:120] + "..." if len(sql) > 120 else sql.replace("\n", ""))

    cur.execute(sql)
