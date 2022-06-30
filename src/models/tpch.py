import glob

from src.database import Query
from src.models.abstract import QTFModel
from src.utils import get_alias_table_names


class TPCHModel(QTFModel):

    def create_tables(self, conn):
        with conn.cursor() as cur:
            with open("sql/tpch/create.sql", "r") as create_sql:
                full_queries = '\n'.join(create_sql.readlines())
                for query in full_queries.split(";"):
                    if cleaned := query.lstrip():
                        cur.execute(cleaned)

    def get_queries(self):
        # todo not fully automated because of sql_metadata lib issues
        tables = ['supplier', 'part', 'partsupp', 'customer', 'orders', 'lineitem', 'nation', 'region']

        queries = []
        query_file_lists = list(glob.glob("sql/tpch/queries/*.sql"))
        for query in query_file_lists:
            with open(query, "r") as query_file:
                full_query = ''.join(query_file.readlines())
                tables_list = get_alias_table_names(full_query)
                queries.append(Query(
                    query=full_query.lstrip(),
                    tables=[alias_t for alias_t, name_t in list(tables_list.items()) if name_t in tables]
                ))

        return queries
