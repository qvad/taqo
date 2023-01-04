import argparse
import glob

import sqlparse

from utils import parse_clear_and_parametrized_sql

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Model query exported to get SQL compilable queries')

    parser.add_argument('--path',
                        help='Path to model directory')

    args = parser.parse_args()

    queries = []
    query_file_lists = sorted(list(glob.glob(f"{args.path}/queries/*.sql")))
    for query in query_file_lists:
        with open(query, "r") as query_file:
            full_queries = ''.join(query_file.readlines())
            for file_query in full_queries.split(";"):
                if cleaned := sqlparse.format(file_query.lstrip(), strip_comments=True).strip():
                    parameters, sql, sql_wo_parameters = parse_clear_and_parametrized_sql(cleaned)

                    queries.append(sql_wo_parameters)

    if queries:
        with open("output.sql", "w") as output:
            output.write('\n'.join([f"{query};" for query in queries]))