import dataclasses
import itertools
from enum import Enum
from typing import List

import psycopg2

from config import Config
from utils import get_explain_clause, evaluate_sql

DEFAULT_USERNAME = 'postgres'
DEFAULT_PASSWORD = 'postgres'

ENABLE_PLAN_HINTING = "SET pg_hint_plan.enable_hint = ON;"
ENABLE_STATISTICS_HINT = "SET yb_enable_optimizer_statistics = true;"


class Connection:
    conn = None

    def __init__(self, connection_config):
        self.connection_config = connection_config

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.connection_config.host,
            port=self.connection_config.port,
            database=self.connection_config.database,
            user=self.connection_config.username,
            password=self.connection_config.password)
        self.conn.autocommit = True

    def get_version(self):
        with self.conn.cursor() as cur:
            evaluate_sql(cur, 'SELECT VERSION();')
            return cur.fetchone()[0]


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


@dataclasses.dataclass
class Field:
    name: str = None
    is_index: bool = None


@dataclasses.dataclass
class Table:
    name: str = None
    fields: List[Field] = None
    size: int = 0


class Leading:
    LEADING = "Leading"

    def __init__(self, tables: List[Table]):
        self.tables = tables
        self.joins = {}
        self.table_scan_hints = []

    def construct(self):
        for tables_perm in itertools.permutations(self.tables):
            prev_el = None
            joins = []
            joined_tables = []

            for table in tables_perm:
                prev_el = f"( {prev_el} {table.name} )" if prev_el else table.name
                joined_tables.append(table.name)

                if prev_el != table.name:
                    if joins:
                        new_joins = [f"{join} {new_join.construct(joined_tables)}"
                                     for join, new_join in itertools.product(joins, Joins)]

                        joins = new_joins
                    else:
                        for new_join in Joins:
                            joins.append(new_join.construct(joined_tables))

            leading_hint = f"{self.LEADING} ({prev_el})"
            self.joins[leading_hint] = joins

        for table in self.tables:
            tables_and_idxs = [f"{Scans.INDEX.value}({table.name})" for field in table.fields if
                               field.is_index]
            tables_and_idxs.append(f"{Scans.SEQ.value}({table.name})")
            self.table_scan_hints.append(tables_and_idxs)


@dataclasses.dataclass
class Query:
    query: str = None
    explain_hints: str = None  # TODO parse possible explain hints?
    tables: List[Table] = None
    execution_plan: str = None
    optimizer_score: float = 1
    optimizer_tips: QueryTips = None
    execution_time_ms: int = 0
    optimizations: List['Query'] = None
    postgres_query: 'Query' = None

    def get_query(self):
        return self.query

    def get_explain(self):
        return f"{get_explain_clause()} {self.query}"

    def get_best_optimization(self):
        best_optimization = self
        for optimization in best_optimization.optimizations:
            if best_optimization.execution_time_ms > optimization.execution_time_ms != 0:
                best_optimization = optimization

        return best_optimization

    def __str__(self):
        return f"Query - \"{self.query}\"\n" \
               f"Tables - \"{self.tables}\"\n" \
               f"Optimization hints - \"{self.explain_hints}\"\n" \
               f"Execution plan - \"{self.execution_plan}\"\n" \
               f"Execution time - \"{self.execution_time_ms}\""


@dataclasses.dataclass
class Optimization(Query):
    def get_query(self):
        return f"/*+ {self.explain_hints} */ {self.query}"

    def get_explain(self):
        return f"{get_explain_clause()}  /*+ {self.explain_hints} */ {self.query}"


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

                    if Config().skip_table_scan_hints:
                        explain_hints = f"{leading} {join}"

                        self.add_optimization(explain_hints, optimizations)
                    else:
                        for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                            if len(optimizations) >= max_optimizations:
                                interrupt = True
                                break

                            explain_hints = f"{leading} {join} {' '.join(table_scan_hint)}"

                            self.add_optimization(explain_hints, optimizations)

                    if interrupt:
                        break

        if not optimizations:
            # case w/o any joins
            for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                if len(optimizations) >= max_optimizations:
                    interrupt = True
                    break

                explain_hints = f"{' '.join(table_scan_hint)}"

                self.add_optimization(explain_hints, optimizations)

        return optimizations

    def add_optimization(self, explain_hints, optimizations):
        skip_optimization = self.filter_optimization_tips(explain_hints)
        if not skip_optimization:
            optimizations.append(
                Optimization(
                    query=self.query.query,
                    explain_hints=explain_hints
                )
            )

    def filter_optimization_tips(self, explain_hints):
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

        return skip_optimization
