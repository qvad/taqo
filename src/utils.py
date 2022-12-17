import hashlib
import math
import re
import time
from copy import copy

import psycopg2
from sql_metadata import Parser

from config import Config

PARAMETER_VARIABLE = r"[^'](\%\((.*?)\))"


def current_milli_time():
    return (time.time_ns() // 1_000) / 1_000


def get_optimizer_score_from_plan(execution_plan):
    try:
        matches = re.finditer(r"\s\(cost=\d+\.\d+\.\.(\d+\.\d+)", execution_plan, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
            return float(match.groups()[0])
    except Exception:
        return 0


def get_result(cur):
    result = cur.fetchall()

    str_result = ""
    cardinality = 0
    for row in result:
        cardinality += 1
        for column_value in row:
            str_result += f"{str(column_value)}"

    return cardinality, str_result


def calculate_avg_execution_time(cur, query, query_str=None, num_retries: int = 0):
    config = Config()

    query_str = query_str or query.get_query()
    query_str_lower = query_str.lower() if query_str is not None else None
    with_analyze = query_str_lower is not None and \
                   "explain" in query_str_lower and \
                   ("analyze" in query_str_lower or "analyse" in query_str_lower)

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
                _, result = get_result(cur)

                # get cardinality for queries with analyze
                evaluate_sql(cur, query.get_query())
                cardinality, _ = get_result(cur)

                matches = re.finditer(r"Execution\sTime:\s(\d+\.\d+)\sms", result, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    result = float(match.groups()[0])
                    break
                sum_execution_times += result
                query.result_cardinality = cardinality
            else:
                sum_execution_times += current_milli_time() - start_time

            if iteration == 0:
                if not result:
                    cardinality, result = get_result(cur)
                    query.result_cardinality = cardinality

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
    return Config().explain_clause


def evaluate_sql(cur, sql):
    config = Config()

    parameters = []

    sql_wo_parameters = copy(sql)
    str_param_skew = 0
    str_wo_param_skew = 0
    for match in re.finditer(PARAMETER_VARIABLE, sql, re.MULTILINE):
        var_value = match.groups()[1]

        if var_value.isnumeric():
            correct_value = int(var_value) if var_value.isdigit() else float(var_value)
        else:
            correct_value = var_value.replace("'", "")

        changed_var_name = '%s'
        sql = changed_var_name.join(
            [sql[:str_param_skew + match.start(1)],
             sql[str_param_skew + match.end(1):]])
        str_param_skew += len(changed_var_name) - (match.end(1) - match.start(1))

        sql_wo_parameters = var_value.join(
            [sql_wo_parameters[:str_wo_param_skew + match.start(1)],
             sql_wo_parameters[str_wo_param_skew + match.end(1):]])
        str_wo_param_skew += len(var_value) - (match.end(1) - match.start(1))

        parameters.append(correct_value)

    config.logger.debug(
        sql.replace("\n", "")[:120] + "..." if len(sql) > 120 else sql.replace("\n", ""))

    if config.parametrized and parameters:
        try:
            cur.execute(sql, parameters)
        except psycopg2.errors.QueryCanceled as e:
            raise e
        except Exception as e:
            config.logger.exception(sql, e)
            raise e
    else:
        try:
            cur.execute(sql_wo_parameters)
        except psycopg2.errors.QueryCanceled as e:
            raise e
        except Exception as e:
            config.logger.exception(sql_wo_parameters, e)
            raise e

    return parameters


def allowed_diff(config, original_execution_time, optimization_execution_time):
    if optimization_execution_time <= 0:
        return False

    return (abs(original_execution_time - optimization_execution_time) /
            optimization_execution_time) < config.skip_percentage_delta


def get_md5(string: str):
    return str(hashlib.md5(string.encode('utf-8')).hexdigest())


def get_bool_from_str(string: str):
    return string in {True, 1, "True", "true", "TRUE", "T"}


def calculate_taqo_score(query):
    """
    Calculates TAQO score based on algorithm from the paper
    https://databasescience.files.wordpress.com/2013/01/taqo.pdf

    Since it doesn't count default execution, it basically tests optimizer score efficiency.
    Not fully representative for this framework.

    :param query: Query object with evaluated optimizations
    :return: float number TAQO score
    """

    optimizations = [q for q in query.optimizations
                     if q and
                     q.optimizer_score is not None
                     and q.execution_time_ms != 0]
    optimizations.sort(key=lambda q: q.execution_time_ms)

    try:
        e_max = max(op.optimizer_score for op in optimizations)
        e_diff = e_max - min(op.optimizer_score for op in optimizations)

        if e_diff == 0:
            e_diff = 0.01

        a_max = max(op.execution_time_ms for op in optimizations)
        a_best = min(op.execution_time_ms for op in optimizations)
        a_diff = a_max - a_best

        score = 0
        for i in range(2, len(optimizations) - 1):
            for j in range(1, i):
                pi = optimizations[j]
                pj = optimizations[i]
                score += (a_best / pi.execution_time_ms) * \
                         (a_best / pj.execution_time_ms) * \
                         math.sqrt(
                             ((pj.execution_time_ms - pi.execution_time_ms) / a_diff) ** 2 + \
                             ((pj.optimizer_score - pi.optimizer_score) / e_diff) ** 2) * \
                         math.copysign(1, (pj.optimizer_score - pi.optimizer_score))
    except InterruptedError as ie:
        raise ie
    except Exception:
        print("Failed to calculate score, setting TAQO score as 0.0")
        return 0.0
    print("{:.2f}".format(score))

    return "{:.2f}".format(score)
