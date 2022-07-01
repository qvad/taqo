import glob
import sqlparse
from sqlparse.sql import Comment

from src.config import Config
from src.database import Query, QueryTips
from src.models.abstract import QTFModel
from src.utils import get_alias_table_names, evaluate_sql


class SQLModel(QTFModel):

    def create_tables(self, conn):
        if Config().skip_model_creation:
            return

        with conn.cursor() as cur:
            with open(f"sql/{Config().model}/create.sql", "r") as create_sql:
                full_queries = '\n'.join(create_sql.readlines())
                for query in full_queries.split(";"):
                    if cleaned := query.lstrip():
                        evaluate_sql(cur, cleaned)

    def get_comments(self, full_query):
        for token in sqlparse.parse(full_query)[0].tokens:
            if isinstance(token, Comment):
                return token.value

    def get_query_hint_tips(self, full_query):
        tips = QueryTips()

        if comments := self.get_comments(full_query):
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

    def get_queries(self):
        # todo not fully automated because of sql_metadata lib issues
        tables = ['supplier', 'part', 'partsupp', 'customer', 'orders', 'lineitem', 'nation',
                  'region']

        queries = []
        query_file_lists = list(glob.glob(f"sql/{Config().model}/queries/*.sql"))
        for query in query_file_lists:
            with open(query, "r") as query_file:
                full_query = ''.join(query_file.readlines())
                tables_list = get_alias_table_names(full_query)
                queries.append(Query(
                    query=sqlparse.format(full_query, strip_comments=True).strip(),
                    tables=[alias_t
                            for alias_t, name_t in list(tables_list.items()) if name_t in tables],
                    optimizer_tips=self.get_query_hint_tips(full_query)
                ))

        return queries
