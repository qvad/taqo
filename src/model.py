import dataclasses
import itertools
from enum import Enum
from pprint import pprint

from database import Query


@dataclasses.dataclass
class Table:
    name: str = None
    size: int = 0


class QueryJoins(Enum):
    INNER = "inner"
    RIGHT_OUTER = "right outer"
    LEFT_OUTER = "left outer"
    # FULL_OUTER = "full outer"
    # CROSS = "cross"


TABLES = [
    Table(f"t{num}", num) for num in [500_000, 100_000, 50_000, 10_000, 1_000, 100]
]


def create_tables(conn):
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table.name}")
            cur.execute(
                f"CREATE TABLE {table.name} as select a, md5(random()::text) from generate_Series(1,{table.size}) a")


def create_queries():
    queries = []

    for perm in itertools.permutations(TABLES, 3):
        first_table = perm[0]
        # TODO proper joins
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
    pprint(create_queries())
