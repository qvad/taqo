import dataclasses
import itertools
import re
from difflib import SequenceMatcher
from enum import Enum
from typing import List, Type

import psycopg2
from allpairspy import AllPairs

from config import Config, ConnectionConfig
from objects import Query, EPNode, ExecutionPlan, ListOfOptimizations, Table, Optimization, \
    ListOfQueries, ResultsLoader
from db.database import Database
from utils import evaluate_sql, allowed_diff

DEFAULT_USERNAME = 'postgres'
DEFAULT_PASSWORD = 'postgres'

ENABLE_PLAN_HINTING = "SET pg_hint_plan.enable_hint = ON;"
ENABLE_DEBUG_HINTING = "SET pg_hint_plan.debug_print = ON;"
CLIENT_MESSAGES_TO_LOG = "SET client_min_messages TO log;"
DEBUG_MESSAGE_LEVEL = "SET pg_hint_plan.message_level = debug;"

PLAN_CLEANUP_REGEX = r"\s\(actual time.*\)|\s\(never executed\)|\s\(cost.*\)|" \
                     r"\sMemory:.*|Planning Time.*|Execution Time.*|Peak Memory Usage.*|" \
                     r"Read RPC Count:.*|Read RPC Wait Time:.*|DocDB Scanned Rows:.*|" \
                     r".*Partial Aggregate:.*|YB\s|Remote\s|" \
                     r"JIT:.*|\s+Functions:.*|\s+Options:.*|\s+Timing:.*"  # PG14 JIT info
PLAN_RPC_CALLS = r"\nRead RPC Count:\s(\d+)"
PLAN_RPC_WAIT_TIMES = r"\nRead RPC Wait Time:\s([+-]?([0-9]*[.])?[0-9]+)"
PLAN_DOCDB_SCANNED_ROWS = r"\nDocDB Scanned Rows:\s(\d+)"
PLAN_PEAK_MEMORY = r"\nPeak memory:\s(\d+)"
PLAN_TREE_CLEANUP = r"\n\s*->\s*|\n\s*"


class Postgres(Database):

    def establish_connection(self, database: str = "postgres"):
        config = ConnectionConfig(
            self.config.connection.host,
            self.config.connection.port,
            self.config.connection.username,
            self.config.connection.password,
            database, )
        self.connection = Connection(config)

        self.connection.connect()

    def get_list_optimizations(self, original_query):
        return PGListOfOptimizations(
            self.config, original_query).get_all_optimizations()

    def get_execution_plan(self, execution_plan: str):
        return PostgresExecutionPlan(execution_plan)

    def get_results_loader(self):
        return PostgresResultsLoader()

    def get_list_queries(self):
        return PostgresListOfQueries()


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


class Scans(Enum):
    SEQ = "SeqScan"
    INDEX = "IndexScan"
    INDEX_ONLY = "IndexOnlyScan"
    # BITMAP = "BitmapScan"


class Joins(Enum):
    HASH = "HashJoin", "Hash"
    MERGE = "MergeJoin", "Merge"
    NESTED_LOOP = "NestLoop", "Nested Loop"

    def construct(self, tables: List[str]):
        return f"{self.value[0]}({' '.join(tables)})"


class Leading:
    LEADING = "Leading"

    def __init__(self, config: Config, tables: List[Table]):
        self.config = config
        self.tables = tables
        self.joins = []
        self.table_scan_hints = []

    def construct(self):
        if self.config.all_pairs_threshold == -1:
            self.get_all_combinations()
        elif len(self.tables) < self.config.all_pairs_threshold:
            self.get_all_combinations()
        else:
            self.get_all_pairs_combinations()

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

    def get_all_pairs_combinations(self):
        if len(self.tables) <= 1:
            return

        # todo to reduce number of pairs combinations used here
        # while its not produce overwhelming amount of optimizations
        # it should provide enough number of combinations
        table_combinations = list(itertools.combinations(self.tables, len(self.tables)))
        join_product = list(AllPairs([list(Joins) for _ in range(len(self.tables) - 1)]))
        scan_product = list(AllPairs([list(Scans) for _ in range(len(self.tables))]))

        for tables, joins, scans in AllPairs([table_combinations, join_product, scan_product]):
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
class PostgresQuery(Query):
    execution_plan: 'PostgresExecutionPlan' = None
    optimizations: List['PostgresOptimization'] = None

    def get_query(self):
        return self.query

    def get_explain(self):
        return f"{Config().explain_clause} {self.query}"

    def get_heuristic_explain(self):
        return f"EXPLAIN {self.query}"

    def get_explain_analyze(self):
        return f"EXPLAIN ANALYZE {self.query}"

    def tips_looks_fair(self, optimization):
        clean_plan = self.execution_plan.get_clean_plan()

        return not any(
            join.value[0] in optimization.explain_hints and join.value[1] not in clean_plan
            for join in Joins)

    def compare_plans(self, execution_plan: Type['ExecutionPlan']):
        return self.execution_plan.get_clean_plan() == \
               self.execution_plan.get_clean_plan(execution_plan)

    def __str__(self):
        return f"Query - \"{self.query}\"\n" \
               f"Tables - \"{self.tables}\"\n" \
               f"Optimization hints - \"{self.explain_hints}\"\n" \
               f"Execution plan - \"{self.execution_plan}\"\n" \
               f"Execution time - \"{self.execution_time_ms}\""

    def heatmap(self):
        config = Config()
        plan_heatmap = {line_id: {'weight': 0, 'str': execution_plan_line}
                        for line_id, execution_plan_line in
                        enumerate(self.execution_plan.get_no_cost_plan().split("->"))}

        best_optimization = self.get_best_optimization(config)
        for optimization in self.optimizations:
            if allowed_diff(config, best_optimization.execution_time_ms,
                            optimization.execution_time_ms):
                no_cost_plan = optimization.execution_plan.get_no_cost_plan()
                for plan_line in plan_heatmap.values():
                    for optimization_line in no_cost_plan.split("->"):
                        if SequenceMatcher(
                                a=optimization.execution_plan.get_no_tree_plan_str(plan_line['str']),
                                b=optimization.execution_plan.get_no_tree_plan_str(optimization_line)
                        ).ratio() > 0.9:
                            plan_line['weight'] += 1

        self.execution_plan_heatmap = plan_heatmap

        return plan_heatmap

    def get_best_optimization(self, config):
        best_optimization = self
        for optimization in best_optimization.optimizations:
            if best_optimization.execution_time_ms < 0:
                best_optimization = optimization
            elif 0 < optimization.execution_time_ms < best_optimization.execution_time_ms:
                best_optimization = optimization

        if allowed_diff(config, best_optimization.execution_time_ms, self.execution_time_ms):
            return self

        return best_optimization


@dataclasses.dataclass
class PostgresOptimization(PostgresQuery, Optimization):
    execution_plan: 'PostgresExecutionPlan' = None

    def get_query(self):
        return f"/*+ {self.explain_hints} */ {self.query}"

    def get_explain(self):
        return f"{Config().explain_clause}  /*+ {self.explain_hints} */ {self.query}"

    def get_heuristic_explain(self):
        return f"EXPLAIN /*+ {self.explain_hints} */ {self.query}"


@dataclasses.dataclass
class PostgresExecutionPlan(ExecutionPlan):
    full_str: str

    def parse_tree(self):
        root = EPNode()
        current_node = root
        for line in self.full_str.split("\n"):
            if line.strip().startswith("->"):
                level = int(line.find("->") / 2)
                previous_node = current_node
                current_node = EPNode()
                current_node.level = level
                current_node.full_str += line

                if previous_node.level <= current_node.level:
                    previous_node.childs.append(current_node)
                    current_node.root = previous_node
                else:
                    walking_node = previous_node.root
                    while walking_node.level != current_node.level:
                        walking_node = walking_node.root
                    walking_node = walking_node.root
                    walking_node.childs.append(current_node)
                    current_node.root = walking_node
            else:
                current_node.full_str += line

    def __cmp__(self, other):
        if isinstance(other, str):
            return self.full_str == other

        return self.full_str == other.full_str

    def __str__(self):
        return self.full_str

    def get_estimated_cost(self):
        try:
            matches = re.finditer(r"\s\(cost=\d+\.\d+\.\.(\d+\.\d+)", self.full_str,
                                  re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                return float(match.groups()[0])
        except Exception as e:
            return 0

    def get_rpc_calls(self, execution_plan: 'ExecutionPlan' = None):
        try:
            return int(re.sub(
                PLAN_RPC_CALLS, '',
                execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_rpc_wait_times(self, execution_plan: 'PostgresExecutionPlan' = None):
        try:
            return int(
                re.sub(PLAN_RPC_WAIT_TIMES, '',
                       execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_scanned_rows(self, execution_plan: 'PostgresExecutionPlan' = None):
        try:
            return int(
                re.sub(PLAN_DOCDB_SCANNED_ROWS, '',
                       execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_peak_memory(self, execution_plan: 'PostgresExecutionPlan' = None):
        try:
            return int(
                re.sub(PLAN_PEAK_MEMORY, '',
                       execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_no_cost_plan(self, execution_plan: 'PostgresExecutionPlan' = None):
        return re.sub(PLAN_CLEANUP_REGEX, '',
                      execution_plan.full_str if execution_plan else self.full_str).strip()

    def get_no_tree_plan(self, execution_plan: 'PostgresExecutionPlan' = None):
        return self.get_no_tree_plan_str(
            execution_plan.full_str if execution_plan else self.full_str)


    @staticmethod
    def get_no_tree_plan_str(plan_str):
        return re.sub(PLAN_TREE_CLEANUP, '\n', plan_str).strip()

    def get_clean_plan(self, execution_plan: Type['ExecutionPlan'] = None):
        no_tree_plan = re.sub(PLAN_TREE_CLEANUP, '\n',
                              execution_plan.full_str if execution_plan else self.full_str).strip()
        return re.sub(PLAN_CLEANUP_REGEX, '', no_tree_plan).strip()


@dataclasses.dataclass
class PGListOfOptimizations(ListOfOptimizations):
    def __init__(self, config: Config, query: PostgresQuery):
        super().__init__(config, query)

        # todo rework this
        self.leading = Leading(self.config, query.tables)
        self.leading.construct()

    def get_all_optimizations(self) -> List[Optimization]:
        optimizations = []
        for leading_join in self.leading.joins:
            for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                explain_hints = f"{leading_join} {' '.join(table_scan_hint)}"

                self.add_optimization(explain_hints, optimizations)

        if not optimizations:
            # case w/o any joins
            for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                explain_hints = f"{' '.join(table_scan_hint)}"

                self.add_optimization(explain_hints, optimizations)

        return optimizations

    def add_optimization(self, explain_hints, optimizations):
        skip_optimization = self.filter_optimization_tips(explain_hints)
        if not skip_optimization:
            optimizations.append(
                Optimization(
                    query=self.query.query,
                    query_hash=self.query.query_hash,
                    explain_hints=explain_hints
                )
            )


class PostgresListOfQueries(ListOfQueries):
    queries: List[PostgresQuery] = None


class PostgresResultsLoader(ResultsLoader):

    def __init__(self):
        super().__init__()
        self.clazz = PostgresListOfQueries
