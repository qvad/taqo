import glob
from typing import List

import sqlparse
from sqlparse.sql import Comment
from tqdm import tqdm

from database import Query, QueryTips, Table, Field
from models.abstract import QTFModel
from utils import get_alias_table_names, evaluate_sql


class SQLModel(QTFModel):

    def create_tables(self, conn):
        created_tables: List[Table] = []

        with conn.cursor() as cur:
            if not self.config.skip_model_creation:
                with open(f"sql/{self.config.model}/create.sql", "r") as create_sql:
                    full_queries = '\n'.join(create_sql.readlines())
                    for query in tqdm(full_queries.split(";")):
                        if cleaned := query.lstrip():
                            evaluate_sql(cur, cleaned)

            self.load_tables_from_public(created_tables, cur)

        return created_tables

    def load_tables_from_public(self, created_tables, cur):
        self.logger.info("Loading tables...")
        cur.execute(
            "select table_name, table_schema from information_schema.tables where table_schema = 'public'")
        tables = []
        result = list(cur.fetchall())
        tables.extend((row[0], row[1])
                      for row in result
                      if row[1] not in ["pg_catalog", "information_schema"])

        self.logger.info("Loading columns and constraints...")
        for table in tables:
            evaluate_sql(
                cur,
                f"""
                        SELECT column_name
                          FROM information_schema.columns
                         WHERE table_schema = '{table[1]}'
                           AND table_name   = '{table[0]}';
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
                            and t.relname like '{table[0]}'
                        order by
                            t.relname,
                            i.relname;
                        """
            )

            fields = []

            result = list(cur.fetchall())
            for column in columns:
                is_indexed = any(column == row[2] for row in result)
                fields.append(Field(column, is_indexed))

            created_tables.append(Table(table[0], fields, 0))

    def get_comments(self, full_query):
        for token in sqlparse.parse(full_query)[0].tokens:
            if isinstance(token, Comment):
                return token.value

    def get_query_hint_tips(self, full_query):
        tips = QueryTips()

        if comments := self.get_comments(full_query).split("\n"):
            for comment_line in comments:
                if comment_line.startswith("-- accept: "):
                    tips.accept = [s.lstrip() for s in
                                   comment_line.replace("-- accept: ", "").split(",")]
                if comment_line.startswith("-- reject: "):
                    tips.reject = [s.lstrip() for s in
                                   comment_line.replace("-- reject: ", "").split(",")]
                if comment_line.startswith("-- max_timeout: "):
                    tips.max_timeout = comment_line.replace("-- max_timeout: ", "").lstrip()

        return tips

    def get_queries(self, tables):
        table_names = [table.name for table in tables]

        queries = []
        query_file_lists = list(glob.glob(f"sql/{self.config.model}/queries/*.sql"))
        for query in query_file_lists:
            with open(query, "r") as query_file:
                full_queries = ''.join(query_file.readlines())
                query_tips = self.get_query_hint_tips(full_queries)
                for query in full_queries.split(";"):
                    if cleaned := query.lstrip():
                        tables_list = get_alias_table_names(cleaned, table_names)
                        queries.append(Query(
                            query=sqlparse.format(cleaned, strip_comments=True).strip(),
                            tables=[table for table in tables if
                                    table.name in tables_list.values()],
                            optimizer_tips=query_tips))

        return queries
