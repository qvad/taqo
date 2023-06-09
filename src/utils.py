import hashlib
import re
import time
import traceback
from copy import copy

import psycopg2
from sql_metadata import Parser

from config import Config
from db.database import Database
from objects import Query

PARAMETER_VARIABLE = r"[^'](\%\((.*?)\))"
WITH_ORDINALITY = r"[Ww][Ii][Tt][Hh]\s*[Oo][Rr][Dd][Ii][Nn][Aa][Ll][Ii][Tt][yY]\s*[Aa][Ss]\s*.*(.*)"
SQL_KEYWORDS = ['GROUP', 'UNION', 'CHECK', 'IS', 'DELETE', 'OUTER', 'IN', 'INTO', 'FROM', 'EXCEPT', 'NOT', 'LEFT',
                'ILIKE', 'JOIN', 'WHERE', 'TABLE', 'VIEW', 'DROP', 'UPDATE', 'DESC', 'UNIQUE', 'ON', 'OR', 'BY', 'SOME',
                'KEY', 'TRUNCATE', 'REPLACE', 'SELECT', 'FULL', 'FOREIGN', 'DISTINCT', 'RIGHT', 'DATABASE', 'INTERSECT',
                'LIMIT', 'DEFAULT', 'ADD', 'ALL', 'BETWEEN', 'INDEX', 'COLUMN', 'EXISTS', 'INSERT', 'SET', 'AS',
                'HAVING', 'INNER', 'ASC', 'LIKE', 'CROSS', 'ORDER', 'CONSTRAINT', 'CASE', 'NULL', 'ANY', 'VALUES',
                'AND', 'PRIMARY', 'ALTER', 'CREATE']


def current_milli_time():
    return (time.time_ns() // 1_000) / 1_000


def remove_with_ordinality(sql_str):
    while True:
        match = re.search(WITH_ORDINALITY, sql_str)
        if not match:
            break

        start, end = match.span(0)
        sql_str = (sql_str[:start] if start > 0 else "") + (
            sql_str[end:] if end < len(sql_str) else "")

    return sql_str


def get_result(cur, is_dml):
    if is_dml:
        return cur.rowcount, f"{cur.rowcount} updates"

    result = cur.fetchall()

    str_result = ""
    cardinality = 0
    for row in result:
        cardinality += 1
        for column_value in row:
            str_result += f"{str(column_value)}"

    return cardinality, str_result


def calculate_avg_execution_time(cur,
                                 query: Query,
                                 sut_database: Database,
                                 query_str: str = None,
                                 num_retries: int = 0,
                                 connection=None) -> object:
    config = Config()

    query_str = query_str or query.get_query()
    query_str_lower = query_str.lower() if query_str is not None else None

    with_analyze = query_with_analyze(query_str_lower)
    is_dml = query_is_dml(query_str_lower)

    sum_execution_times = 0
    actual_evaluations = 0

    # run at least one iteration
    num_retries = max(num_retries, 2)
    num_warmup = config.num_warmup

    for iteration in range(num_retries + num_warmup):
        # noinspection PyUnresolvedReferences
        try:
            sut_database.prepare_query_execution(cur)
            start_time = current_milli_time()
            query.parameters = evaluate_sql(cur, query_str)

            if iteration == 0:
                cardinality, result = get_result(cur, is_dml)
                if with_analyze:
                    query.result_cardinality = extract_actual_cardinality(result)
                    query.result_hash = "NONE"
                else:
                    query.result_cardinality = cardinality
                    query.result_hash = get_md5(result)
            elif iteration >= num_warmup:
                if with_analyze:
                    _, result = get_result(cur, is_dml)

                    sum_execution_times += extract_execution_time_from_analyze(result)
                else:
                    sum_execution_times += current_milli_time() - start_time
        except psycopg2.errors.QueryCanceled:
            # failed by timeout - it's ok just skip optimization
            query.execution_time_ms = -1
            config.logger.debug(
                f"Skipping optimization due to timeout limitation:\n{query_str}")
            return False
        except psycopg2.errors.DatabaseError as ie:
            # Some serious problem occurred - probably an issue
            query.execution_time_ms = 0
            config.logger.error(f"INTERNAL ERROR {ie}\nSQL query:\n{query_str}")
            traceback.print_exc(limit=None, file=None, chain=True)
            return False
        finally:
            connection.rollback()

            if iteration >= num_warmup:
                actual_evaluations += 1

    query.execution_time_ms = sum_execution_times / actual_evaluations

    return True


def query_with_analyze(query_str_lower):
    return query_str_lower is not None and \
        "explain" in query_str_lower and \
        ("analyze" in query_str_lower or "analyse" in query_str_lower)


def query_is_dml(query_str_lower):
    return "update" in query_str_lower or \
        "insert" in query_str_lower or \
        "delete" in query_str_lower


def extract_execution_time_from_analyze(result):
    extracted = -1
    matches = re.finditer(r"Execution\sTime:\s(\d+\.\d+)\sms", result, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
        extracted = float(match.groups()[0])
        break

    return extracted


def extract_actual_cardinality(result):
    matches = re.finditer(r"\(actual\stime.*rows=(\d+).*\)", result.split("\n")[0], re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
        result = float(match.groups()[0])
        break

    return result


def check_alias_validity(alias: str):
    upper_alias = alias.upper()

    if " " in upper_alias:
        return False

    return upper_alias not in SQL_KEYWORDS


def get_alias_table_names(sql_str, tables_in_sut):
    table_names = [table.name for table in tables_in_sut]

    # 'WITH ORDINALITY' clauses get misinterpreted as
    # aliases so remove them from the query. 
    parser = Parser(remove_with_ordinality(sql_str))

    # todo this code contains too many magic
    # todo so it has more smell rather that magic
    # 'where' may occur in table_name_in_query aliases_in_query
    tables_in_query = parser.tables
    aliases_in_query = parser.tables_aliases

    result_tables = {alias: table_name for alias, table_name in aliases_in_query.items()
                     if check_alias_validity(alias) and table_name in table_names}

    if not len(result_tables) == len(tables_in_query) == len(aliases_in_query):
        # add tables w/o aliases
        for table in tables_in_query:
            if table not in result_tables.keys():
                result_tables[table] = table

    # return usable table objects list
    table_objects_in_query = []
    for alias, table_name_in_query in result_tables.items():
        for real_table in tables_in_sut:
            if table_name_in_query == real_table.name \
                    or ('.' in table_name_in_query
                        and table_name_in_query.split(".")[1] == real_table.name):
                real_table.alias = alias
                table_objects_in_query.append(real_table)

    return table_objects_in_query


def evaluate_sql(cur, sql):
    config = Config()

    parameters, sql, sql_wo_parameters = parse_clear_and_parametrized_sql(sql)

    config.logger.debug(sql)

    if config.parametrized and parameters:
        try:
            cur.execute(sql, parameters)
        except psycopg2.errors.QueryCanceled as e:
            cur.connection.rollback()
            raise e
        except psycopg2.errors.ConfigurationLimitExceeded as cle:
            cur.connection.rollback()
            config.logger.exception(sql, cle)
        except psycopg2.OperationalError as oe:
            cur.connection.rollback()
            config.logger.exception(sql, oe)
        except Exception as e:
            cur.connection.rollback()
            config.logger.exception(sql_wo_parameters, e)
            raise e
    else:
        try:
            cur.execute(sql_wo_parameters)
        except psycopg2.errors.QueryCanceled as e:
            cur.connection.rollback()
            raise e
        except psycopg2.errors.ConfigurationLimitExceeded as cle:
            cur.connection.rollback()
            config.logger.exception(sql, cle)
        except psycopg2.OperationalError as oe:
            cur.connection.rollback()
            config.logger.exception(sql, oe)
        except Exception as e:
            cur.connection.rollback()
            config.logger.exception(sql_wo_parameters, e)
            raise e

    return parameters


def parse_clear_and_parametrized_sql(sql):
    parameters = []
    sql_wo_parameters = copy(sql)
    str_param_skew = 0
    str_wo_param_skew = 0
    changed_var_name = '%s'

    for match in re.finditer(PARAMETER_VARIABLE, sql, re.MULTILINE):
        var_value = match.groups()[1]

        if var_value.isnumeric():
            correct_value = int(var_value) if var_value.isdigit() else float(var_value)
        else:
            correct_value = var_value.replace("'", "")

        sql = changed_var_name.join(
            [sql[:str_param_skew + match.start(1)],
             sql[str_param_skew + match.end(1):]])
        str_param_skew += len(changed_var_name) - (match.end(1) - match.start(1))

        sql_wo_parameters = var_value.join(
            [sql_wo_parameters[:str_wo_param_skew + match.start(1)],
             sql_wo_parameters[str_wo_param_skew + match.end(1):]])
        str_wo_param_skew += len(var_value) - (match.end(1) - match.start(1))

        parameters.append(correct_value)

    return parameters, sql, sql_wo_parameters


def allowed_diff(config, original_execution_time, optimization_execution_time):
    if optimization_execution_time <= 0:
        return False

    return (abs(original_execution_time - optimization_execution_time) /
            optimization_execution_time) < config.skip_percentage_delta


def get_md5(string: str):
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def get_bool_from_object(string: str | bool | int):
    return string in {True, 1, "True", "true", "TRUE", "T"}


def disabled_path(query):
    return query.execution_plan.get_estimated_cost() < 10000000000
