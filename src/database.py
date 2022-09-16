import dataclasses
import itertools
import json
import re
from enum import Enum
from typing import List, Dict

import psycopg2
from allpairspy import AllPairs
from dacite import Config as DaciteConfig
from dacite import from_dict

from config import Config
from utils import get_explain_clause, evaluate_sql

DEFAULT_USERNAME = 'postgres'
DEFAULT_PASSWORD = 'postgres'

ENABLE_PLAN_HINTING = "SET pg_hint_plan.enable_hint = ON;"
ENABLE_DEBUG_HINTING = "SET pg_hint_plan.debug_print = ON;"
CLIENT_MESSAGES_TO_LOG = "SET client_min_messages TO log;"
DEBUG_MESSAGE_LEVEL = "SET pg_hint_plan.message_level = debug;"
ENABLE_STATISTICS_HINT = "SET yb_enable_optimizer_statistics = true;"

PLAN_CLEANUP_REGEX = r"\s\(actual time.*\)|\s\(never executed\)|\s\(cost.*\)|" \
                     r"\sMemory:.*|Planning Time.*|Execution Time.*|Peak Memory Usage.*|" \
                     r"Read RPC Count:.*|Read RPC Wait Time:.*|DocDB Scanned Rows:.*|"
PLAN_RPC_CALLS = r"\nRead RPC Count:\s(\d+)"
PLAN_RPC_WAIT_TIMES = r"\nRead RPC Wait Time:\s([+-]?([0-9]*[.])?[0-9]+)"
PLAN_DOCDB_SCANNED_ROWS = r"\nDocDB Scanned Rows:\s(\d+)"
PLAN_PEAK_MEMORY = r"\nPeak memory:\s(\d+)"
PLAN_TREE_CLEANUP = r"\n\s*->\s*|\n\s*"


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
    # INDEX_ONLY = "IndexOnlyScan"
    # BITMAP = "BitmapScan"


class Joins(Enum):
    HASH = "HashJoin", "Hash"
    MERGE = "MergeJoin", "Merge"
    NESTED_LOOP = "NestLoop", "Nested Loop"

    def construct(self, tables: List[str]):
        return f"{self.value[0]}({' '.join(tables)})"


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

    def __init__(self, config: Config, tables: List[Table]):
        self.config = config
        self.tables = tables
        self.joins = []
        self.table_scan_hints = []

    def construct(self):
        if self.config.use_allpairs:
            self.fill_joins_with_pairwise()
        else:
            self.get_all_combinations()

    def get_all_combinations(self):
        # algorithm with all possible combinations
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

            for join in joins:
                self.joins.append(f"{self.LEADING} ({prev_el}) {join}")

        for table in self.tables:
            tables_and_idxs = [f"{Scans.INDEX.value}({table.name})" for field in table.fields if
                               field.is_index]
            tables_and_idxs.append(f"{Scans.SEQ.value}({table.name})")
            self.table_scan_hints.append(tables_and_idxs)

    def fill_joins_with_pairwise(self):
        table_permutations = list(itertools.permutations(self.tables))
        join_product = list(itertools.product(Joins, repeat=len(self.tables) - 1))
        scan_product = list(itertools.product(Scans, repeat=len(self.tables)))

        for tables, joins, scans in AllPairs([table_permutations, join_product, scan_product]):
            prev_el = None
            joins = itertools.cycle(joins)
            query_joins = ""
            joined_tables = []

            for table in tables:
                prev_el = f"( {prev_el} {table.name} )" if prev_el else table.name
                joined_tables.append(table.name)

                if prev_el != table.name:
                    query_joins += f" {next(joins).construct(joined_tables)}"

            leading_hint = f"{self.LEADING} ({prev_el})"
            scan_hints = " ".join(
                f"{scan.value}({self.tables[table_idx].name})" for table_idx, scan in
                enumerate(scans))

            self.joins.append(f"{leading_hint} {query_joins} {scan_hints}")


@dataclasses.dataclass
class Query:
    tag: str = ""
    query: str = ""
    explain_hints: str = ""
    tables: List[Table] = None
    execution_plan: str = ""
    execution_plan_heatmap: Dict[int, Dict[str, str]] = None
    optimizer_score: float = 1
    optimizer_tips: QueryTips = None
    execution_time_ms: float = 0
    optimizations: List['Query'] = None
    postgres_query: 'Query' = None
    result_hash: 'Query' = None

    def get_query(self):
        return self.query

    def get_explain(self):
        return f"{get_explain_clause()} {self.query}"

    def get_heuristic_explain(self):
        return f"EXPLAIN {self.query}"

    def get_explain_analyze(self):
        return f"EXPLAIN ANALYZE {self.query}"

    def get_best_optimization(self):
        best_optimization = self
        for optimization in best_optimization.optimizations:
            if best_optimization.execution_time_ms > optimization.execution_time_ms != 0:
                best_optimization = optimization

        return best_optimization

    def get_rpc_calls(self, execution_plan=None):
        try:
            return int(re.sub(PLAN_RPC_CALLS, '', execution_plan or self.execution_plan).strip())
        except Exception:
            return 0

    def get_rpc_wait_times(self, execution_plan=None):
        try:
            return int(
                re.sub(PLAN_RPC_WAIT_TIMES, '', execution_plan or self.execution_plan).strip())
        except Exception:
            return 0

    def get_scanned_rows(self, execution_plan=None):
        try:
            return int(
                re.sub(PLAN_DOCDB_SCANNED_ROWS, '', execution_plan or self.execution_plan).strip())
        except Exception:
            return 0

    def get_peak_memory(self, execution_plan=None):
        try:
            return int(re.sub(PLAN_PEAK_MEMORY, '', execution_plan or self.execution_plan).strip())
        except Exception:
            return 0

    def get_no_cost_plan(self, execution_plan=None):
        return re.sub(PLAN_CLEANUP_REGEX, '', execution_plan or self.execution_plan).strip()

    def get_no_tree_plan(self, execution_plan=None):
        return re.sub(PLAN_TREE_CLEANUP, '\n', execution_plan or self.execution_plan).strip()

    def get_clean_plan(self, execution_plan=None):
        return self.get_no_tree_plan(self.get_no_cost_plan(execution_plan=execution_plan))

    def tips_looks_fair(self, optimization):
        clean_plan = self.get_clean_plan()

        return not any(
            join.value[0] in optimization.explain_hints and join.value[1] not in clean_plan
            for join in Joins)

    def compare_plans(self, execution_plan):
        return self.get_clean_plan() == self.get_clean_plan(execution_plan)

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

    def get_heuristic_explain(self):
        return f"EXPLAIN /*+ {self.explain_hints} */ {self.query}"


@dataclasses.dataclass
class ListOfQueries:
    db_version: str = ""
    git_message: str = ""
    queries: List[Query] = None

    def append(self, new_element):
        if not self.queries:
            self.queries = [new_element, ]
        else:
            self.queries.append(new_element)


class ListOfOptimizations:
    def __init__(self, config: Config, query: Query):
        self.query = query
        self.leading = Leading(config, query.tables)
        self.leading.construct()

    def get_all_optimizations(self, max_optimizations) -> List[Optimization]:
        optimizations = []
        for leading_join in self.leading.joins:
            if Config().skip_table_scan_hints or Config().use_allpairs:
                self.add_optimization(leading_join, optimizations)
            else:
                for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                    if len(optimizations) >= max_optimizations:
                        break

                    explain_hints = f"{leading_join} {' '.join(table_scan_hint)}"

                    self.add_optimization(explain_hints, optimizations)

        if not optimizations:
            # case w/o any joins
            for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                if len(optimizations) >= max_optimizations:
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


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def get_queries_from_previous_result(previous_execution_path):
    with open(previous_execution_path, "r") as prev_result:
        return from_dict(ListOfQueries, json.load(prev_result), DaciteConfig(check_types=False))


def store_queries_to_file(queries):
    with open("report/output.json", "w") as result_file:
        result_file.write(json.dumps(queries, cls=EnhancedJSONEncoder))
