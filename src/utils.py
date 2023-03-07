import hashlib
import re
import time
from copy import copy

import psycopg2
from sql_metadata import Parser

from config import Config
from objects import Query

PARAMETER_VARIABLE = r"[^'](\%\((.*?)\))"


def current_milli_time():
    return (time.time_ns() // 1_000) / 1_000


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
            start_time = current_milli_time()
            query.parameters = evaluate_sql(cur, query_str)

            result = None
            if iteration >= num_warmup and with_analyze:
                _, result = get_result(cur, is_dml)
                connection.rollback()

                # get cardinality for queries with analyze
                evaluate_sql(cur, query.get_query())
                cardinality, _ = get_result(cur, is_dml)
                connection.rollback()

                sum_execution_times += extract_execution_time_from_analyze(result)
                query.result_cardinality = cardinality
            else:
                sum_execution_times += current_milli_time() - start_time
                connection.rollback()

            if iteration == 0:
                if not result:
                    cardinality, result = get_result(cur, is_dml)
                    query.result_cardinality = cardinality
                    connection.rollback()

                query.result_hash = get_md5(result)
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
            return False
        finally:
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
    matches = re.finditer(r"Execution\sTime:\s(\d+\.\d+)\sms", result, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
        result = float(match.groups()[0])
        break

    return result


def check_alias_validity(alias: str):
    lower_alias = alias.lower()

    if " " in lower_alias:
        return False

    return lower_alias not in {'on', 'where', 'group by', 'from'}


def get_alias_table_names(sql_str, tables_in_sut):
    table_names = [table.name for table in tables_in_sut]

    parser = Parser(sql_str)

    # todo some workarounds for sql_metadata package issues
    # 'where' may occur in table_name_in_query aliases_in_query
    tables_in_query = parser.tables
    aliases_in_query = parser.tables_aliases

    result_tables = {alias: table_name for alias, table_name in aliases_in_query.items()
                     if check_alias_validity(alias) and table_name in table_names}

    # add tables w/o aliases
    for table in tables_in_query:
        if table not in result_tables.keys():
            result_tables[table] = table

    # return usable table objects list
    table_objects_in_query = []
    # todo major issue here that mans that taoq doesn't work with aliases
    for alias, table_name_in_query in result_tables.items():
        table_objects_in_query.extend(
            real_table
            for real_table in tables_in_sut
            if table_name_in_query == real_table.name
            or ('.' in table_name_in_query
                and table_name_in_query.split(".")[1] == real_table.name))

    return table_objects_in_query


def evaluate_sql(cur, sql):
    config = Config()

    parameters, sql, sql_wo_parameters = parse_clear_and_parametrized_sql(sql)

    config.logger.debug(sql)

    if config.parametrized and parameters:
        try:
            cur.execute(sql, parameters)
        except psycopg2.errors.QueryCanceled as e:
            raise e
        except psycopg2.errors.ConfigurationLimitExceeded as cle:
            config.logger.exception(sql, cle)
        except psycopg2.OperationalError as oe:
            config.logger.exception(sql, oe)
            cur = cur.connection.cursor()
        except Exception as e:
            config.logger.exception(sql, e)
            raise e
    else:
        try:
            cur.execute(sql_wo_parameters)
        except psycopg2.errors.QueryCanceled as e:
            raise e
        except psycopg2.errors.ConfigurationLimitExceeded as cle:
            config.logger.exception(sql, cle)
        except psycopg2.OperationalError as oe:
            config.logger.exception(sql, oe)
            cur = cur.connection.cursor()
        except Exception as e:
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
    return str(hashlib.md5(string.encode('utf-8')).hexdigest())


def get_bool_from_str(string: str):
    return string in {True, 1, "True", "true", "TRUE", "T"}
