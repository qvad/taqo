import glob
import os
import re
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
from utils import get_alias_table_names, evaluate_sql, get_md5, get_model_path


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
                created_tables = self.load_tables_from_public(cur)

        self.load_table_stats(conn.cursor(), created_tables)

        return created_tables, teardown_queries + create_queries + analyze_queries + import_queries

    def evaluate_ddl_queries(self, conn,
                             step_prefix: DDLStep,
                             db_prefix=None):
        self.logger.info(f"Evaluating DDL {step_prefix.name} step")

        created_tables: List[Table] = []
        file_name = step_prefix.name.lower()

        db_prefix = self.config.ddl_prefix or db_prefix
        if db_prefix and exists(f"{get_model_path(self.config.model)}/{db_prefix}.{file_name}.sql"):
            file_name = f"{db_prefix}.{file_name}"

        model_queries = []
        try:
            with conn.cursor() as cur:
                evaluate_sql(cur, f"SET statement_timeout = '{self.config.ddl_query_timeout}s'")

                path_to_file = f"{get_model_path(self.config.model)}/{file_name}.sql"

                if not exists(path_to_file):
                    self.logger.warn(f"Unable to locate file {path_to_file}")
                else:
                    with open(f"{get_model_path(self.config.model)}/{file_name}.sql", "r") as sql_file:
                        full_queries = self.apply_variables('\n'.join(sql_file.readlines()))
                        for query in tqdm(full_queries.split(";")):
                            try:
                                if cleaned := query.lstrip():
                                    model_queries.append(cleaned)
                                    if step_prefix == DDLStep.IMPORT:
                                        self.import_from_local(cur, cleaned)
                                    else:
                                        evaluate_sql(cur, cleaned)
                            except psycopg2.Error as e:
                                self.logger.exception(e)
                                raise e
                if step_prefix == DDLStep.CREATE:
                    created_tables = self.load_tables_from_public(cur)

            return created_tables, model_queries
        except Exception as e:
            self.logger.exception(e)
            raise e

    def import_from_local(self, cur, cleaned):
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

    def load_tables_from_public(self, cur):
        created_tables = []

        load_catalog_clause = " or table_schema = 'pg_catalog'" if self.config.load_catalog_tables else ""

        self.logger.info("Loading tables...")
        cur.execute(
            f"""
            select table_name, table_schema 
            from information_schema.tables 
            where table_schema = 'public' {load_catalog_clause};
            """)
        tables = []
        result = list(cur.fetchall())
        tables.extend((row[0], row[1])
                      for row in result
                      if row[1] not in ["information_schema"])

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
                    indexes = list(row[1] for row in result if column == row[2])
                    fields.append(Field(column, is_indexed, indexes))
            except Exception as e:
                self.logger.exception(result, e)

            created_tables.append(Table(name=table_name, fields=fields, rows=0, size=0))

        return created_tables

    def load_table_stats(self, cur, tables):
        self.logger.info("Loading table statistics...")
        tmap = {}
        for t in tables:
            if t.name in tmap:
                raise AssertionError(f"Found multiple tables with the same name: {t.name}")
            tmap[t.name] = t

        evaluate_sql(
            cur,
            f"""
            select
                c.relname table_name,
                c.reltuples as rows
            from
                pg_class c,
                pg_namespace ns
            where
                ns.oid = c.relnamespace
                and c.relkind = 'r'
                and ns.nspname in ('public', 'pg_catalog')
                and c.relname =any(array{list(tmap)});
                 """
             )

        tstats = cur.fetchall()

        for ts in tstats:
            tmap[ts[0]].rows = ts[1]

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
                    if comment_line.startswith("-- debug_hints: "):
                        tips.debug_hints = comment_line.replace("-- debug_hints: ", "").strip()

        return tips

    def get_queries(self, tables):
        queries = []
        query_file_lists = sorted(list(glob.glob(f"{get_model_path(self.config.model)}/queries/*.sql")))
        for query in query_file_lists:
            with open(query, "r") as query_file:
                full_queries = self.apply_variables(''.join(query_file.readlines()))
                query_tips = self.get_query_hint_tips(full_queries)
                for file_query in full_queries.split(";"):
                    if cleaned := sqlparse.format(file_query.lstrip(), strip_comments=True).strip():
                        tables_in_query = get_alias_table_names(cleaned, tables)
                        queries.append(PostgresQuery(
                            tag=os.path.basename(query).replace(".sql", ""),
                            query=cleaned,
                            query_hash=get_md5(cleaned),
                            tables=tables_in_query,
                            optimizer_tips=query_tips))

        if self.config.num_queries > 0:
            queries = queries[:int(self.config.num_queries)]

        return queries

    def apply_variables(self, queries_str):
        variables = {
            '$MULTIPLIER': self.config.basic_multiplier,
            "$DATA_PATH": self.config.remote_data_path
        }

        for variable_name, variable_value in variables.items():
            if variable_value:
                queries_str = queries_str.replace(variable_name,
                                                  str(variable_value))

        return queries_str
