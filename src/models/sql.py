import glob
import os
import random
import string
from os.path import exists
from typing import List

import psycopg2
import sqlparse
from sqlparse.sql import Comment
from tqdm import tqdm

from config import DDLStep
from objects import QueryTips, Field
from db.postgres import PostgresQuery, Table
from models.abstract import QTFModel
from utils import get_alias_table_names, evaluate_sql, get_md5


class SQLModel(QTFModel):

    def create_tables(self, conn, skip_analyze=False, db_prefix=None):
        teardown_queries = []
        create_queries = []
        analyze_queries = []
        import_queries = []
        created_tables = []

        if DDLStep.DROP in self.config.ddls:
            _, teardown_queries = self.evaluate_ddl_queries(conn, DDLStep.DROP, db_prefix)
            teardown_queries.insert(0, "-- DROP QUERIES")

        if DDLStep.CREATE in self.config.ddls:
            created_tables, create_queries = self.evaluate_ddl_queries(conn, DDLStep.CREATE,
                                                                       db_prefix)
            create_queries.insert(0, "-- CREATE QUERIES")

        if DDLStep.IMPORT in self.config.ddls:
            _, import_queries = self.evaluate_ddl_queries(conn, DDLStep.IMPORT, db_prefix)
            import_queries.insert(0, "-- IMPORT QUERIES")

        if DDLStep.ANALYZE in self.config.ddls:
            analyzed_tables, analyze_queries = self.evaluate_ddl_queries(conn, DDLStep.ANALYZE,
                                                                         db_prefix)
            create_queries.insert(0, "-- ANALYZE QUERIES")

        if not created_tables:
            # try to load current tables
            with conn.cursor() as cur:
                self.load_tables_from_public(created_tables, cur)

        return created_tables, teardown_queries + create_queries + analyze_queries + import_queries

    def evaluate_ddl_queries(self, conn, step_prefix: DDLStep,
                             db_prefix=None):
        self.logger.info(f"Evaluating DDL {step_prefix.name} step")

        created_tables: List[Table] = []
        file_name = step_prefix.name.lower()

        db_prefix = self.config.ddl_prefix or db_prefix
        if db_prefix and exists(f"sql/{self.config.model}/{db_prefix}.{file_name}.sql"):
            file_name = f"{db_prefix}.{file_name}"

        if step_prefix == DDLStep.IMPORT:
            self.generate_data()

        model_queries = []
        try:
            with conn.cursor() as cur:
                path_to_file = f"sql/{self.config.model}/{file_name}.sql"

                if not exists(path_to_file):
                    self.logger.warn(f"Unable to locate file {path_to_file}")
                else:
                    with open(f"sql/{self.config.model}/{file_name}.sql", "r") as sql_file:
                        full_queries = self.apply_variables('\n'.join(sql_file.readlines()))
                        for query in tqdm(full_queries.split(";")):
                            try:
                                if cleaned := query.lstrip():
                                    model_queries.append(cleaned)
                                    evaluate_sql(cur, cleaned)
                            except psycopg2.Error as e:
                                self.logger.exception(e)
                                raise e
                if step_prefix == DDLStep.CREATE:
                    self.load_tables_from_public(created_tables, cur)
        except Exception as e:
            self.logger.exception(e)
            exit(1)
        finally:
            return created_tables, model_queries

    def load_tables_from_public(self, created_tables, cur):
        self.logger.info("Loading tables...")
        cur.execute(
            """
            select table_name, table_schema 
            from information_schema.tables 
            where table_schema = 'public';
            """)
        tables = []
        result = list(cur.fetchall())
        tables.extend((row[0], row[1])
                      for row in result
                      if row[1] not in ["pg_catalog", "information_schema"])

        self.logger.info("Loading columns and constraints...")
        for table_name, schema_name in tables:
            evaluate_sql(
                cur,
                f"""
                select column_name
                from information_schema.columns
                where table_schema = '{schema_name}'
                and table_name  = '{table_name}';
                """
            )

            columns = [row[0] for row in list(cur.fetchall())]

            evaluate_sql(
                cur,
                f"""
                select
                    t.relname as table_name,
                    i.relname as index_name,
                    a.attname as column_name
                from
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a
                where
                    t.oid = ix.indrelid
                    and i.oid = ix.indexrelid
                    and a.attrelid = t.oid
                    and a.attnum = ANY(ix.indkey)
                    and t.relkind = 'r'
                    and t.relname like '{table_name}'
                order by
                    t.relname,
                    i.relname;
                """
            )

            fields = []

            result = list(cur.fetchall())
            try:
                for column in columns:
                    is_indexed = any(column == row[2] for row in result)
                    fields.append(Field(column, is_indexed))
            except Exception as e:
                self.logger.exception(result, e)

            created_tables.append(Table(table_name, fields, 0))

    @staticmethod
    def get_comments(full_query):
        for token in sqlparse.parse(full_query)[0].tokens:
            if isinstance(token, Comment):
                return token.value

    def get_query_hint_tips(self, full_query):
        tips = QueryTips()

        query_comments = self.get_comments(full_query)
        if query_comments is not None:
            if comments := query_comments.split("\n"):
                for comment_line in comments:
                    if comment_line.startswith("-- accept: "):
                        tips.accept = [s.strip() for s in
                                       comment_line.replace("-- accept: ", "").split(",")]
                    if comment_line.startswith("-- reject: "):
                        tips.reject = [s.strip() for s in
                                       comment_line.replace("-- reject: ", "").split(",")]
                    if comment_line.startswith("-- tags: "):
                        tips.tags = [s.strip() for s in
                                       comment_line.replace("-- tags: ", "").split(",")]
                    if comment_line.startswith("-- max_timeout: "):
                        tips.max_timeout = comment_line.replace("-- max_timeout: ", "").strip()

        return tips

    def get_queries(self, tables):
        table_names = [table.name for table in tables]

        queries = []
        query_file_lists = sorted(list(glob.glob(f"sql/{self.config.model}/queries/*.sql")))
        for query in query_file_lists:
            with open(query, "r") as query_file:
                full_queries = self.apply_variables(''.join(query_file.readlines()))
                query_tips = self.get_query_hint_tips(full_queries)
                for file_query in full_queries.split(";"):
                    if cleaned := sqlparse.format(file_query.lstrip(), strip_comments=True).strip():
                        tables_list = get_alias_table_names(cleaned, table_names)
                        queries.append(PostgresQuery(
                            tag=os.path.basename(query).replace(".sql", ""),
                            query=cleaned,
                            query_hash=get_md5(cleaned),
                            tables=[table for table in tables if
                                    table.name in tables_list.values()],
                            optimizer_tips=query_tips))

        if self.config.num_queries > 0:
            queries = queries[:int(self.config.num_queries)]

        return queries

    def apply_variables(self, queries_str):
        if self.config.remote_data_path:
            return queries_str.replace("$DATA_PATH",
                                       self.config.remote_data_path)
        else:
            return queries_str.replace("$DATA_PATH",
                                       f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data")


class BasicOpsModel(SQLModel):

    def generate_data(self):
        self.logger.info("Generating data files for simplified model")

        # create dir if not there yet
        if not exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data"):
            os.mkdir(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data")

        self.create_data_for_50kx_table('t1', 16)
        self.create_data_for_50kx_table('t2', 128)
        self.create_data_for_50kx_table('t3', 512)

        self.create_table_with_1k_nulls('ts2', 20000)
        self.create_table_with_1k_nulls('ts3', 5000)

    def create_data_for_50kx_table(self, table_name: str, str_length: int):
        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/{table_name}.csv"):
            self.logger.warn(f"Model files already presented, skipping {table_name}.csv")
        else:
            with open(
                    f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/{table_name}.csv",
                    "w") as csv_file:
                for i in tqdm(range(50_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(
                        random.choices(string.ascii_uppercase + string.digits, k=str_length))
                    csv_file.write(f"{i},k2-{i},{i},{ng_string}\n")

    def create_table_with_1k_nulls(self, table_name: str, table_size: int):
        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/{table_name}.csv"):
            self.logger.warn(f"Model files already presented, skipping {table_name}.csv")
        else:
            with open(
                    f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/{table_name}.csv",
                    "w") as table_file:
                for i in tqdm(range((table_size - 3000) * self.config.basic_multiplier)):
                    ng_string = ''.join(
                        random.choices(string.ascii_uppercase + string.digits, k=16))
                    table_file.write(f"{i},k2-{i},{i},{ng_string}\n")

                for i in tqdm(range((table_size - 3000) * self.config.basic_multiplier,
                                    (table_size - 2000) * self.config.basic_multiplier)):
                    ng_string = ''.join(
                        random.choices(string.ascii_uppercase + string.digits, k=16))
                    table_file.write(f"{i},k2-{i},NULL,{ng_string}\n")

                for i in tqdm(range((table_size - 2000) * self.config.basic_multiplier,
                                    (table_size - 1000) * self.config.basic_multiplier)):
                    table_file.write(f"{i},k2-{i},{i},NULL\n")

                for i in tqdm(range((table_size - 1000) * self.config.basic_multiplier,
                                    table_size * self.config.basic_multiplier)):
                    table_file.write(f"{i},k2-{i},NULL,NULL\n")
