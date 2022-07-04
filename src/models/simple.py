import itertools

from enum import Enum
from pprint import pprint

from src.config import Config
from src.database import Query, Table, Field
from src.models.abstract import QTFModel
from src.utils import evaluate_sql


class QueryJoins(Enum):
    INNER = "INNER"
    RIGHT_OUTER = "RIGHT OUTER"
    LEFT_OUTER = "LEFT OUTER"
    FULL_OUTER = "FULL"
    # CROSS = "cross"


class SimpleModel(QTFModel):
    TABLES = [
        Table(f"t{num}", [Field('a', True), Field('md5', False)], num) for num in
        [1_000_000, 500_000, 50_000, 100]
    ]

    def create_tables(self, conn):
        if Config().skip_model_creation:
            return self.TABLES

        if Config().verbose:
            print("Creating simple model tables and run analyze")

        with conn.cursor() as cur:
            for table in self.TABLES:
                evaluate_sql(cur, f"DROP TABLE IF EXISTS {table.name}")
                evaluate_sql(
                    cur,
                    f"CREATE TABLE {table.name} as select a, md5(random()::text) from generate_Series(1,{table.size}) a")
                evaluate_sql(
                    cur,
                    f"CREATE INDEX {table.name}_idx ON {table.name}(a)")

                evaluate_sql(cur, f"ANALYZE {table.name}")

        return self.TABLES

    def get_queries(self, tables):
        queries = []

        where_clauses = itertools.cycle([
            "in", "<", ">"
        ])
        limit_clauses = itertools.cycle([
            "", "limit"
        ])

        for perm in itertools.permutations(tables, 3):
            first_table = perm[0]
            for query_join in QueryJoins:
                query = f"SELECT * FROM {first_table.name} "
                for table in perm[1:]:
                    query += f" {query_join.value} join {table.name}" \
                             f" on {first_table.name}.a = {table.name}.a"

                query += " where"
                min_size = min(tb.size for tb in perm)

                # where clause types
                next_where_expression_type = next(where_clauses)
                if next_where_expression_type == "<":
                    query += f" {first_table.name}.a < {min_size}"
                elif next_where_expression_type == ">":
                    query += f" {first_table.name}.a > {int(min_size / 2)}"
                elif next_where_expression_type == "in":
                    query += f" {first_table.name}.a in ({','.join([str(n) for n in range(100)])})"
                else:
                    raise AttributeError(
                        f"Unknown where expression type {next_where_expression_type}")

                # limit clause types
                next_limit_clause = next(limit_clauses)
                if next_limit_clause == "limit":
                    query += f" {next_limit_clause} {min(1000, min_size)}"

                queries.append(Query(
                    query=query,
                    tables=list(perm)
                ))

        return queries


if __name__ == "__main__":
    pprint(SimpleModel().get_queries())
