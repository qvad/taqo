import re
import time
import psycopg2


def current_milli_time():
    return round(time.time() * 1000)


def get_optimizer_score_from_plan(execution_plan):
    matches = re.finditer(r"cost=.*\.\.(\d+\.\d+)", execution_plan, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
        return float(match.groups()[0])


def calculate_avg_execution_time(cur, query, num_retries):
    sum_execution_times = 0
    actual_evaluations = 0

    for _ in range(num_retries):
        try:
            start_time = current_milli_time()
            cur.execute(query.query)
            sum_execution_times += current_milli_time() - start_time
        except psycopg2.errors.QueryCanceled:
            # failed by timeout - it's ok
            sum_execution_times += 0
        else:
            sum_execution_times += current_milli_time() - start_time
        finally:
            actual_evaluations += 1

    query.execution_time_ms = sum_execution_times / actual_evaluations