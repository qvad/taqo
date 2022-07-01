import dataclasses
import itertools

from enum import Enum
from pprint import pprint

from src.config import Config
from src.database import Query
from src.models.abstract import QTFModel
from src.utils import evaluate_sql


@dataclasses.dataclass
class Table:
    name: str = None
    size: int = 0


class QueryJoins(Enum):
    INNER = "INNER"
    RIGHT_OUTER = "RIGHT OUTER"
    LEFT_OUTER = "LEFT OUTER"
    FULL_OUTER = "FULL"
    # CROSS = "cross"


class SimpleModel(QTFModel):
    TABLES = [
        Table(f"t{num}", num) for num in [1_000_000, 500_000, 50_000, 100]
    ]

    def create_tables(self, conn):
        if Config().skip_model_creation:
            return

        if Config().verbose:
            print("Creating simple model tables and run analyze")

        with conn.cursor() as cur:
            for table in self.TABLES:
                evaluate_sql(cur, f"DROP TABLE IF EXISTS {table.name}")
                evaluate_sql(
                    cur,
                    f"CREATE TABLE {table.name} as select a, md5(random()::text) from generate_Series(1,{table.size}) a")

                evaluate_sql(cur, f"ANALYZE {table.name}")

    def get_queries(self):
        queries = []

        for perm in itertools.permutations(self.TABLES, 3):
            first_table = perm[0]
            for query_join in QueryJoins:
                query = f"SELECT * FROM {first_table.name} "
                for table in perm[1:]:
                    query += f" {query_join.value} join {table.name} on {first_table.name}.a = {table.name}.a"

                queries.append(Query(
                    query=query,
                    tables=[tb.name for tb in perm]
                ))

        return queries


if __name__ == "__main__":
    pprint(SimpleModel().get_queries())
