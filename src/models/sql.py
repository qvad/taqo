import glob
import os
import random
import shutil
import string
from os.path import exists
from typing import List

import sqlparse
from sqlparse.sql import Comment
from tqdm import tqdm

from database import Query, QueryTips, Table, Field
from models.abstract import QTFModel
from utils import get_alias_table_names, evaluate_sql


class SQLModel(QTFModel):

    def create_tables(self, conn, skip_analyze=False, db_prefix=None):
        created_tables: List[Table] = []

        if db_prefix and exists(f"sql/{self.config.model}/{db_prefix}.create.sql"):
            file_name = f"{db_prefix}.create"
        else:
            file_name = "create"

        self.generate_data()

        model_queries = []
        with conn.cursor() as cur:
            if not self.config.skip_model_creation:
                with open(f"sql/{self.config.model}/{file_name}.sql", "r") as create_sql:
                    full_queries = self.apply_variables('\n'.join(create_sql.readlines()))
                    for query in tqdm(full_queries.split(";")):
                        if cleaned := query.lstrip():
                            if skip_analyze and 'analyze' in cleaned.lower():
                                continue

                            model_queries.append(cleaned)
                            evaluate_sql(cur, cleaned)

            self.load_tables_from_public(created_tables, cur)

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
        for table in tables:
            evaluate_sql(
                cur,
                f"""
                select column_name
                from information_schema.columns
                where table_schema = '{table[1]}'
                and table_name   = '{table[0]}';
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

        query_comments = self.get_comments(full_query)
        if query_comments is not None:
            if comments := query_comments.split("\n"):
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
        query_file_lists = sorted(list(glob.glob(f"sql/{self.config.model}/queries/*.sql")))
        for query in query_file_lists:
            with open(query, "r") as query_file:
                full_queries = self.apply_variables(''.join(query_file.readlines()))
                query_tips = self.get_query_hint_tips(full_queries)
                for file_query in full_queries.split(";"):
                    if cleaned := sqlparse.format(file_query.lstrip(), strip_comments=True).strip():
                        tables_list = get_alias_table_names(cleaned, table_names)
                        queries.append(Query(
                            tag=os.path.basename(query).replace(".sql", ""),
                            query=cleaned,
                            tables=[table for table in tables if
                                    table.name in tables_list.values()],
                            optimizer_tips=query_tips))

        if self.config.num_queries > 0:
            queries = queries[:int(self.config.num_queries)]

        return queries

    def apply_variables(self, queries_str):
        return queries_str.replace("$DATA_PATH",
                                   f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}")


class BasicOpsModel(SQLModel):

    def generate_data(self):
        self.logger.info("Generating data files for simplified model")

        # create dir if not there yet
        if not exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data"):
            os.mkdir(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data")

        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/t1.csv"):
            self.logger.warn("Model files already presented, skipping t1.csv")
        else:
            with open(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/t1.csv", "w") as t1_file:
                for i in tqdm(range(50_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
                    t1_file.write(f"{i},k2-{i},{i},{ng_string}\n")

        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/t2.csv"):
            self.logger.warn("Model files already presented, skipping t2.csv")
        else:
            with open(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/t2.csv", "w") as t2_file:
                for i in tqdm(range(50_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=128))
                    t2_file.write(f"{i},k2-{i},{i},{ng_string}\n")

        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/t3.csv"):
            self.logger.warn("Model files already presented, skipping t3.csv")
        else:
            with open(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/t3.csv", "w") as t3_file:
                for i in tqdm(range(50_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=512))
                    t3_file.write(f"{i},k2-{i},{i},{ng_string}\n")

        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/ts2.csv"):
            self.logger.warn("Model files already presented, skipping ts2.csv")
        else:
            with open(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/ts2.csv", "w") as ts2_file:
                for i in tqdm(range(17_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
                    ts2_file.write(f"{i},k2-{i},{i},{ng_string}\n")

                for i in tqdm(range(17_000 * self.config.basic_multiplier, 18_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
                    ts2_file.write(f"{i},k2-{i},NULL,{ng_string}\n")

                for i in tqdm(range(18_000 * self.config.basic_multiplier, 19_000 * self.config.basic_multiplier)):
                    ts2_file.write(f"{i},k2-{i},{i},NULL\n")

                for i in tqdm(range(19_000 * self.config.basic_multiplier, 20_000 * self.config.basic_multiplier)):
                    ts2_file.write(f"{i},k2-{i},NULL,NULL\n")

        if exists(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/ts3.csv"):
            self.logger.warn("Model files already presented, skipping ts3.csv")
        else:
            with open(f"{os.path.abspath(os.getcwd())}/sql/{self.config.model}/data/ts3.csv", "w") as ts3_file:
                for i in tqdm(range(2_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
                    ts3_file.write(f"{i},k2-{i},{i},{ng_string}\n")

                for i in tqdm(range(2_000 * self.config.basic_multiplier, 3_000 * self.config.basic_multiplier)):
                    ng_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
                    ts3_file.write(f"{i},k2-{i},NULL,{ng_string}\n")

                for i in tqdm(range(3_000 * self.config.basic_multiplier, 4_000 * self.config.basic_multiplier)):
                    ts3_file.write(f"{i},k2-{i},{i},NULL\n")

                for i in tqdm(range(4_000 * self.config.basic_multiplier, 5_000 * self.config.basic_multiplier)):
                    ts3_file.write(f"{i},k2-{i},NULL,NULL\n")
