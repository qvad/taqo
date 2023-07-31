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
        _, teardown_queries = self.evaluate_ddl_queries(conn, DDLStep.DROP, DDLStep.DROP in self.config.ddls, db_prefix)
        teardown_queries.insert(0, "-- DROP QUERIES")

        created_tables, create_queries = self.evaluate_ddl_queries(conn,
                                                                   DDLStep.CREATE,
                                                                   DDLStep.CREATE in self.config.ddls,
                                                                   db_prefix)
        create_queries.insert(0, "-- CREATE QUERIES")

        _, import_queries = self.evaluate_ddl_queries(conn,
                                                      DDLStep.IMPORT,
                                                      DDLStep.IMPORT in self.config.ddls,
                                                      db_prefix)
        import_queries.insert(0, "-- IMPORT QUERIES")


        analyzed_tables, analyze_queries = self.evaluate_ddl_queries(conn,
                                                                     DDLStep.ANALYZE,
                                                                     DDLStep.ANALYZE in self.config.ddls,
                                                                     db_prefix)
        analyze_queries.insert(0, "-- ANALYZE QUERIES")

        if not created_tables:
            # try to load current tables
            with conn.cursor() as cur:
                created_tables = self.load_tables_from_public(cur)

        self.load_table_stats(conn.cursor(), created_tables)

        return created_tables, teardown_queries, create_queries, analyze_queries, import_queries

    def evaluate_ddl_queries(self, conn,
                             step_prefix: DDLStep,
                             do_execute: bool,
                             db_prefix=None,):
        self.logger.info(f"Evaluating DDL {step_prefix.name} step")

        created_tables: List[Table] = []
        file_name = step_prefix.name.lower()

        db_prefix = self.config.ddl_prefix or db_prefix
        if db_prefix and exists(f"{get_model_path(self.config.model)}/{db_prefix}.{file_name}.sql"):
            file_name = f"{db_prefix}.{file_name}"

        model_queries = []
        try:
            with conn.cursor() as cur:
                if do_execute:
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
                                    if do_execute:
                                        if step_prefix == DDLStep.IMPORT:
                                            self.import_from_local(cur, cleaned)
                                        else:
                                            evaluate_sql(cur, cleaned)
                            except psycopg2.Error as e:
                                self.logger.exception(e)
                                raise e
                if step_prefix == DDLStep.CREATE:
                    if do_execute:
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
        catalog_schema = ", 'pg_catalog'" if self.config.load_catalog_tables else ""

        # we are assuming no table name conflicts between public and pg_catalog schemas for now.
        # column_width (defined width) is -1 for text, array type, etc. types with unbound length)
        self.logger.info("Loading tables, columns and indexes...")
        evaluate_sql(
            cur,
            f"""
            select
                relname as table_name,
                attname as column_name,
                attnum as column_position,
                case when attlen > 0 then attlen else atttypmod end column_width,
                coalesce(index_names, '{{}}') as index_names
            from
                pg_namespace nc
                join pg_class c on nc.oid = relnamespace
                join pg_attribute a on attrelid = c.oid
                left join (
                    select
                        array_agg(relname) as index_names,
                        indrelid,
                        keycol
                    from (
                        select relname, indrelid, unnest(indkey) keycol
                        from pg_index ix join pg_class ci on ix.indexrelid = ci.oid
                    ) indexes
                    group by
                        indrelid,
                        keycol
                ) i on i.indrelid = c.oid
                       and i.keycol = a.attnum
            where
                relkind = 'r'
                and attnum >= 0
                and nspname in ('public'{catalog_schema})
            order by
                nspname,
                relname,
                attnum;
            """)

        created_tables = []
        table = Table()
        for tname, cname, cpos, clen, inames in cur.fetchall():
            if tname != table.name:
                table = Table(name=tname, fields=[], rows=0, size=0)
                created_tables.append(table)

            table.fields.append(Field(name=cname, position=cpos,
                                      is_index=(inames != None),
                                      indexes=inames,
                                      defined_width=clen))

        return created_tables

    def load_table_stats(self, cur, tables):
        catalog_schema = ", 'pg_catalog'" if self.config.load_catalog_tables else ""

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
                and ns.nspname in ('public'{catalog_schema})
                and c.relname =any(array{list(tmap)});
                 """
             )

        for tname, rows in cur.fetchall():
            tmap[tname].rows = rows


        self.logger.info("Loading column statistics...")
        evaluate_sql(
            cur,
            f"""
            select
                c.relname as table_name,
                a.attname as column_name,
                a.attnum as column_position,
                s.stawidth as avg_width
            from
                pg_namespace ns
                join pg_class c on ns.oid = c.relnamespace
                join pg_attribute a on a.attrelid = c.oid
                left join pg_statistic s on s.starelid = c.oid
                                            and a.attnum = s.staattnum
            where
                c.relkind = 'r'
                and a.attnum > 0
                and ns.nspname in ('public'{catalog_schema})
                and c.relname =any(array{list(tmap)});
                 """
             )

        for tname, cname, cpos, cwidth in cur.fetchall():
            if cwidth:
                field = tmap[tname].fields[cpos-1]
                if field.name != cname or field.position != cpos:
                    raise AssertionError(''.join([
                        f"Field position mismatch in table {tname}:",
                        f" the fields[{cpos-1}] should be {cname}",
                        f" but {field.name} and its position={field.position}"]))
                field.avg_width = cwidth

        self.logger.debug("Loaded table and column metadata:")
        for t in tables:
            self.logger.debug(f"{t}")

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
