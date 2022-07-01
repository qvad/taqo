import dataclasses
import itertools

from enum import Enum
from pprint import pprint
from typing import List

from src.config import Config

EXPLAIN = "EXPLAIN "
EXPLAIN_ANALYZE = "EXPLAIN ANALYZE "
ENABLE_HINT = "SET pg_hint_plan.enable_hint = ON;"


@dataclasses.dataclass
class QueryTips:
    accept: List[str] = dataclasses.field(default_factory=list)
    reject: List[str] = dataclasses.field(default_factory=list)
    max_timeout: str = dataclasses.field(default_factory=str)


class Scans(Enum):
    SEQ = "SeqScan"
    INDEX = "IndexScan"
    INDEX_ONLY = "IndexOnlyScan"
    BITMAP = "BitmapScan"


class Joins(Enum):
    HASH = "HashJoin"
    MERGE = "MergeJoin"
    NESTED_LOOP = "NestLoop"

    def construct(self, tables: List[str]):
        return f"{self.value}({' '.join(tables)})"


class Leading:
    LEADING = "Leading"

    def __init__(self, tables: List[str]):
        self.tables = tables
        self.joins = {}

    def construct(self):
        for tables_perm in itertools.permutations(self.tables):
            prev_el = None
            joins = []
            joined_tables = []

            for table in tables_perm:
                prev_el = f"( {prev_el} {table} )" if prev_el else table
                joined_tables.append(table)

                if prev_el != table:
                    if joins:
                        new_joins = [f"{join} {new_join.construct(joined_tables)}"
                                     for join, new_join in itertools.product(joins, Joins)]

                        joins = new_joins
                    else:
                        for new_join in Joins:
                            joins.append(new_join.construct(joined_tables))

            leading_hint = f"{self.LEADING} ({prev_el})"
            self.joins[leading_hint] = joins


@dataclasses.dataclass
class Optimization:
    query: str = None
    explain_hints: str = None
    execution_plan: str = None
    optimizer_score: float = 1
    execution_time_ms: int = 0

    def get_query(self):
        return f"/*+ {self.explain_hints} */ {self.query}"

    def get_explain(self):
        explain = EXPLAIN_ANALYZE if Config().explain_analyze else EXPLAIN
        return f"{explain}  /*+ {self.explain_hints} */ {self.query}"

    def __str__(self):
        return f"Query - \"{self.query}\"\n" \
               f"Optimization hints - \"{self.explain_hints}\"\n" \
               f"Execution plan - \"{self.execution_plan}\"\n" \
               f"Execution time - \"{self.execution_time_ms}\""


@dataclasses.dataclass
class Query:
    query: str = None
    explain_hints: str = None  # TODO parse possible explain hints?
    tables: List[str] = None
    execution_plan: str = None
    optimizer_score: float = 1
    optimizer_tips: QueryTips = None
    execution_time_ms: int = 0

    def get_query(self):
        return self.query

    def get_explain(self):
        explain = EXPLAIN_ANALYZE if Config().explain_analyze else EXPLAIN
        return f"{explain} {self.query}"

    def __str__(self):
        return f"Query - \"{self.query}\"\n" \
               f"Tables - \"{self.tables}\"\n" \
               f"Execution plan - \"{self.execution_plan}\"\n" \
               f"Execution time - \"{self.execution_time_ms}\""


class ListOfOptimizations:
    def __init__(self, query: Query):
        self.query = query
        self.leading = Leading(query.tables)
        self.leading.construct()

    def get_all_optimizations(self, max_optimizations) -> List[Optimization]:
        optimizations = []
        interrupt = False
        for leading, joins in self.leading.joins.items():
            if not interrupt:
                for join in joins:
                    if len(optimizations) >= max_optimizations:
                        interrupt = True
                        break

                    # todo refactor this
                    explain_hints = f"{leading} {join}"
                    skip_optimization = False
                    if self.query.optimizer_tips:
                        for accept_tip in self.query.optimizer_tips.accept:
                            if accept_tip not in explain_hints:
                                skip_optimization = True
                                break
                        if not skip_optimization:
                            for reject_tip in self.query.optimizer_tips.reject:
                                if reject_tip in explain_hints:
                                    skip_optimization = True
                                    break

                    if not skip_optimization:
                        optimizations.append(
                            Optimization(
                                query=self.query.query,
                                explain_hints=f"{leading} {join}"
                            )
                        )

        return optimizations


if __name__ == "__main__":
    ld = Leading(['a', 'b', 'c', 'd'])
    ld.construct()

    pprint(ld.joins, width=600)
