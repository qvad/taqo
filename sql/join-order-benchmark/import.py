import argparse
import re
from tqdm import tqdm

import psycopg2


def import_from_local(cur, cleaned):
    copy_re = r"(?i)\bCOPY\b\s(.+)\s\bFROM\b\s\'(.*)\'\s\bWITH\b\s\((.*\,?)\)"
    parse_re = re.findall(copy_re, cleaned, re.MULTILINE)[0]
    table_name = parse_re[0]
    local_path = parse_re[1]
    params = parse_re[2]

    delimiter = ","
    file_format = None
    null_format = ''
    if 'delimiter' in params.lower():
        delimiter = re.findall(r"(?i)delimiter\s\'(.{1,3})\'", params)[0]
        if delimiter == "\\t":
            delimiter = "\t"
    if 'format' in params.lower():
        file_format = re.findall(r"(?i)format\s([a-zA-Z]+)", params)[0]
    if 'null' in params.lower():
        null_format = re.findall(r"(?i)null\s\'([a-zA-Z]+)\'", params)[0]

    if 'csv' not in file_format.lower():
        raise AttributeError("Can't import from non CSV files")

    with open(local_path, "r") as csv_file:
        cur.copy_from(csv_file, table_name,
                      sep=delimiter,
                      null=null_format)


def apply_variables(data_path, queries_str):
    variables = {
        "$DATA_PATH": data_path
    }

    for variable_name, variable_value in variables.items():
        if variable_value:
            queries_str = queries_str.replace(variable_name,
                                              str(variable_value))

    return queries_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Query Optimizer Testing framework for PostgreSQL compatible DBs')

    parser.add_argument('--host',
                        default="127.0.0.1",
                        help='Target host IP for postgres compatible database')
    parser.add_argument('--port',
                        default=5433,
                        help='Target port for postgres compatible database')
    parser.add_argument('--username',
                        default="yugabyte",
                        help='Username for connection')
    parser.add_argument('--password',
                        default="yugabyte",
                        help='Password for user for connection')
    parser.add_argument('--database',
                        default="taqo",
                        help='Target database in postgres compatible database')

    parser.add_argument('--import_file',
                        default="import.sql",
                        help='Import file path')
    parser.add_argument('--data_path',
                        help='Data folder path')

    args = parser.parse_args()

    with psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.username,
            password=args.password) as conn:
        with conn.cursor() as cur:
            with open(f"{args.import_file}", "r") as sql_file:
                full_queries = apply_variables(args.data_path, '\n'.join(sql_file.readlines()))
                for query in tqdm(full_queries.split(";")):
                    try:
                        if cleaned := query.lstrip():
                            import_from_local(cur, cleaned)
                    except Exception as e:
                        raise e
