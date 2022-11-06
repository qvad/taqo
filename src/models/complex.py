import hashlib
import itertools
import random

from tqdm import tqdm

from config import ModelSteps
from database import Query, Table, Field
from models.abstract import QTFModel, QueryJoins
from utils import evaluate_sql, get_md5


class ComplexModel(QTFModel):
    TABLES = [
        Table(f"t{num}",
              [
                  Field('c_int', True),
                  Field('c_bool', True),
                  Field('c_text', True),
                  Field('c_varchar', True),
                  Field('c_decimal', True),
                  Field('c_float', True),
                  Field('c_real', True),
                  Field('c_money', True)
              ], num) for num in
        [1_000_000, 500_000, 50_000, 100]
        # [50_000, 5_000, 100]
    ]
    COLUMNS = [
        'c_int',
        'c_text',
        'c_varchar',
        'c_decimal',
        'c_float',
        'c_real',
        'c_money',
    ]
    INDEXED_AND_SELECTED = [
        ["c_int"],
        ["c_int", "c_bool"],
        ["c_int", "c_text"],
        ["c_int", "c_varchar"],
        ["c_float", "c_text", "c_varchar"],
        ["c_float", "c_decimal", "c_varchar"],
        ["c_float", "c_real", "c_money"],
    ]

    # IS NOT NULL

    def create_tables(self, conn, skip_analyze=False, db_prefix=None):
        if len(self.config.model_creation) == 0:
            return self.TABLES

        self.logger.info("Creating simple model tables and run analyze")

        model_queries = []
        with conn.cursor() as cur:
            for table in tqdm(self.TABLES):
                colocation = "" if db_prefix else "WITH (colocated = true)"

                if ModelSteps.TEARDOWN:
                    evaluate_sql(cur, f"DROP TABLE IF EXISTS {table.name} CASCADE")

                if ModelSteps.CREATE and ModelSteps.IMPORT:
                    create_table = f"CREATE TABLE {table.name} {colocation} AS " \
                                   f"SELECT c_int, " \
                                   f"(case when c_int % 2 = 0 then true else false end) as c_bool, " \
                                   f"(c_int + 0.0001)::text as c_text, " \
                                   f"(c_int + 0.0002)::varchar as c_varchar, " \
                                   f"(c_int + 0.1)::decimal as c_decimal, " \
                                   f"(c_int + 0.2)::float as c_float, " \
                                   f"(c_int + 0.3)::real as c_real, " \
                                   f"(c_int + 0.4)::money as c_money " \
                                   f"FROM generate_Series(1,{table.size}) c_int;"
                    evaluate_sql(cur, create_table)

                    model_queries.append(create_table)
                    for columns_list in self.INDEXED_AND_SELECTED:
                        joined_columns_list = ', '.join(columns_list)
                        hex_digest = get_md5(joined_columns_list)

                        create_index = f"CREATE INDEX {table.name}_{hex_digest}_idx " \
                                       f"ON {table.name}({joined_columns_list})"
                        evaluate_sql(cur, create_index)
                        model_queries.append(create_index)

                    if not skip_analyze:
                        evaluate_sql(cur, f"ANALYZE {table.name}")

        return self.TABLES, model_queries

    def get_queries(self, tables):
        random.seed(self.config.random_seed)
        queries = []

        selected_columns = itertools.cycle(self.INDEXED_AND_SELECTED)
        columns = itertools.cycle(self.COLUMNS)
        distinct = itertools.cycle([
            "", "DISTINCT"
        ])
        where_clauses = itertools.cycle([
            "IN", "<", ">"
        ])
        order_clauses = itertools.cycle([
            "ASC", "DESC"
        ])
        limit_clauses = itertools.cycle([
            "", "LIMIT"
        ])
        offset_clauses = itertools.cycle([
            "", "10", "50"
        ])

        self.logger.info("Generating queries for test")

        for perm in itertools.permutations(tables, 3):
            first_table = perm[0]
            tables_cycle = itertools.cycle(perm)
            for query_join in QueryJoins:
                next_distinct = next(distinct)
                joined_columns_list = ', '.join(
                    [f"{next(tables_cycle).name}.{column}" for column in next(selected_columns)])
                query = f"SELECT {next_distinct} {joined_columns_list} FROM {first_table.name} "
                first_column = next(columns)

                for table in perm[1:]:
                    query += f" {query_join.value} JOIN {table.name}" \
                             f" ON {first_table.name}.{first_column} = {table.name}.{first_column}"

                query += " WHERE"
                min_size = min(tb.size for tb in perm)
                max_size = max(tb.size for tb in perm)

                # where clause types
                next_where_expression_type = next(where_clauses)
                if next_where_expression_type == "<":
                    query += f" {first_table.name}.c_int {next_where_expression_type} {random.randint(1, min_size)}"
                elif next_where_expression_type == ">":
                    query += f" {first_table.name}.c_int {next_where_expression_type} {random.randint(1, int(min_size / 2))}"
                elif next_where_expression_type == "IN":
                    query += f" {first_table.name}.c_int {next_where_expression_type} ({','.join([str(random.randint(1, max_size)) for _ in range(50)])})"
                else:
                    raise AttributeError(
                        f"Unknown where expression type {next_where_expression_type}")

                # group by clauses
                if next_order_by := next(order_clauses):
                    order_cols = joined_columns_list if next_distinct else f"{first_table.name}.c_int"
                    query += f" ORDER BY {order_cols} {next_order_by}"

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
