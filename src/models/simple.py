import itertools
from enum import Enum

from tqdm import tqdm

from database import Query, Table, Field
from models.abstract import QTFModel
from utils import evaluate_sql


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
        if self.config.skip_model_creation:
            return self.TABLES

        self.logger.info("Creating simple model tables and run analyze")

        with conn.cursor() as cur:
            for table in tqdm(self.TABLES):
                evaluate_sql(cur, f"DROP TABLE IF EXISTS {table.name}")
                evaluate_sql(
                    cur,
                    f"CREATE TABLE {table.name} AS SELECT a, md5(random()::text) FROM generate_Series(1,{table.size}) a")
                evaluate_sql(
                    cur,
                    f"CREATE INDEX {table.name}_idx ON {table.name}(a)")

                evaluate_sql(cur, f"ANALYZE {table.name}")

        return self.TABLES

    def get_queries(self, tables):
        queries = []

        where_clauses = itertools.cycle([
            "IN", "<", ">"
        ])
        order_clauses = itertools.cycle([
            "", "ASC", "DESC"
        ])
        limit_clauses = itertools.cycle([
            "", "LIMIT"
        ])
        offset_clauses = itertools.cycle([
            "", "10", "50"
        ])

        self.logger.info(f"Generating {self.config.num_queries} queries for test")

        for perm in itertools.permutations(tables, 3):
            first_table = perm[0]
            for query_join in QueryJoins:
                query = f"SELECT * FROM {first_table.name} "

                for table in perm[1:]:
                    query += f" {query_join.value} JOIN {table.name}" \
                             f" ON {first_table.name}.a = {table.name}.a"

                query += " WHERE"
                min_size = min(tb.size for tb in perm)

                # where clause types
                next_where_expression_type = next(where_clauses)
                if next_where_expression_type == "<":
                    query += f" {first_table.name}.a {next_where_expression_type} {min_size}"
                elif next_where_expression_type == ">":
                    query += f" {first_table.name}.a {next_where_expression_type} {int(min_size / 2)}"
                elif next_where_expression_type == "IN":
                    query += f" {first_table.name}.a {next_where_expression_type} ({','.join([str(n) for n in range(100)])})"
                else:
                    raise AttributeError(
                        f"Unknown where expression type {next_where_expression_type}")

                # group by clauses
                if next_order_by := next(order_clauses):
                    query += f" ORDER BY {first_table.name}.a {next_order_by}"

                # limit clause types
                if limit_clause := next(limit_clauses):
                    query += f" {limit_clause} {min(1000, min_size)}"

                # offset clauses
                if offset := next(offset_clauses):
                    query += f" OFFSET {offset}"

                queries.append(Query(
                    query=query,
                    tables=list(perm)
                ))

        if self.config.num_queries > 0:
            queries = queries[:int(self.config.num_queries)]

        return queries
