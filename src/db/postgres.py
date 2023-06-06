import dataclasses
import itertools
import re
from difflib import SequenceMatcher
from enum import Enum
from typing import List, Type

import psycopg2
from allpairspy import AllPairs

from config import Config, ConnectionConfig, DDLStep
from objects import Query, EPNode, ExecutionPlan, ListOfOptimizations, Table, Optimization, \
    CollectResult, ResultsLoader
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

    def prepare_query_execution(self, cur):
        for query in self.config.session_props:
            evaluate_sql(cur, query)

    def create_test_database(self):
        if DDLStep.DATABASE in self.config.ddls:
            self.establish_connection("postgres")
            conn = self.connection.conn
            try:
                with conn.cursor() as cur:
                    colocated = "" if self.config.ddl_prefix else " WITH COLOCATED = true"
                    evaluate_sql(cur, f'CREATE DATABASE {self.config.connection.database}{colocated};')
            except Exception as e:
                self.logger.exception(f"Failed to create testing database {e}")

    def get_list_optimizations(self, original_query):
        return PGListOfOptimizations(
            self.config, original_query).get_all_optimizations()

    def get_execution_plan(self, execution_plan: str):
        return PostgresExecutionPlan(execution_plan)

    def get_results_loader(self):
        return PostgresResultsLoader()

    def get_list_queries(self):
        return PostgresCollectResult()


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

    def __init__(self, config: Config, alias_to_table: List[Table]):
        self.config = config
        self.alias_to_table = alias_to_table
        self.joins = []
        self.table_scan_hints = []

    def construct(self):
        if self.config.all_pairs_threshold == -1:
            self.get_all_combinations()
        elif len(self.alias_to_table) < self.config.all_pairs_threshold:
            self.get_all_combinations()
        elif len(self.alias_to_table) == self.config.all_pairs_threshold:
            self.get_all_pairs_with_all_table_permutations()
        else:
            self.get_all_pairs_combinations()

    def filtered_permutations(self, tables):
        # todo check how it works
        perms = list(itertools.permutations(tables))

        if len(tables) < self.config.all_pairs_threshold:
            return perms

        combs = list(itertools.combinations(tables, len(tables) - 1))

        result = []
        for perm in perms:
            perm_join = "".join([table.name for table in perm])
            for comb in combs:
                comb_join = "".join([table.name for table in comb])
                if comb_join in perm_join:
                    result.append(perm)

        return result

    def get_all_combinations(self):
        # algorithm with all possible combinations
        for tables_perm in itertools.permutations(self.alias_to_table):
            prev_el = None
            joins = []
            joined_tables = []

            for alias_to_table in tables_perm:
                alias = alias_to_table.alias

                prev_el = f"( {prev_el} {alias} )" if prev_el else alias
                joined_tables.append(alias)

                if prev_el != alias:
                    if joins:
                        new_joins = [f"{join} {new_join.construct(joined_tables)}"
                                     for join, new_join in itertools.product(joins, Joins)]

                        joins = new_joins
                    else:
                        for new_join in Joins:
                            joins.append(new_join.construct(joined_tables))

            for join in joins:
                self.joins.append(f"{self.LEADING} ( {prev_el} ) {join}")

        for table in self.alias_to_table:
            tables_and_idxs = list({f"{Scans.INDEX.value}({table.alias})"
                                    for field in table.fields if field.is_index})
            tables_and_idxs += {f"{Scans.INDEX_ONLY.value}({table.alias})"
                                for field in table.fields if field.is_index}
            tables_and_idxs.append(f"{Scans.SEQ.value}({table.alias})")
            self.table_scan_hints.append(tables_and_idxs)

    def get_all_pairs_with_all_table_permutations(self):
        # algorithm with all possible table permutations
        # but with all pairs scans
        table_permutations = list(itertools.permutations(self.alias_to_table))
        join_product = list(AllPairs([list(Joins) for _ in range(len(self.alias_to_table) - 1)]))
        scan_product = list(AllPairs([list(Scans) for _ in range(len(self.alias_to_table))]))

        for tables, joins, scans in AllPairs([table_permutations, join_product, scan_product]):
            prev_el = None
            joins = itertools.cycle(joins)
            query_joins = ""
            joined_tables = []

            for table in tables:
                prev_el = f"( {prev_el} {table.alias} )" if prev_el else table.alias
                joined_tables.append(table.alias)

                if prev_el != table.alias:
                    query_joins += f" {next(joins).construct(joined_tables)}"

            leading_hint = f"{self.LEADING} ({prev_el})"
            scan_hints = " ".join(
                f"{scan.value}({tables[table_idx].alias})" for table_idx, scan in
                enumerate(scans))

            self.joins.append(f"{leading_hint} {query_joins} {scan_hints}")

    def get_all_pairs_combinations(self):
        if len(self.alias_to_table) <= 1:
            return

        scan_product = list(AllPairs([list(Scans) for _ in range(len(self.alias_to_table))]))

        for scans in scan_product:
            scan_hints = " ".join(
                f"{scan.value}({self.alias_to_table[table_idx].alias})" for table_idx, scan in enumerate(scans))

            self.joins.append(f"{scan_hints}")


@dataclasses.dataclass
class PostgresQuery(Query):
    execution_plan: 'PostgresExecutionPlan' = None
    optimizations: List['PostgresOptimization'] = None

    def get_debug_hints(self):
        return f"/*+ {self.optimizer_tips.debug_hints} */ " if self.optimizer_tips.debug_hints else ""

    def get_query(self):
        return f"{self.get_debug_hints()}{self.query}"

    def get_explain(self):
        return f"{Config().explain_clause} {self.get_query()}"

    def get_heuristic_explain(self):
        return f"EXPLAIN {self.get_query()}"

    def get_explain_analyze(self):
        return f"EXPLAIN ANALYZE {self.get_query()}"

    def tips_looks_fair(self, optimization):
        clean_plan = self.execution_plan.get_clean_plan()

        return not any(
            join.value[0] in optimization.explain_hints and join.value[1] not in clean_plan
            for join in Joins)

    def compare_plans(self, execution_plan: Type['ExecutionPlan']):
        if clean_plan := self.execution_plan.get_clean_plan():
            return clean_plan == self.execution_plan.get_clean_plan(execution_plan)
        else:
            return False

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
        if self.optimizations:
            for optimization in self.optimizations:
                if allowed_diff(config, best_optimization.execution_time_ms,
                                optimization.execution_time_ms):
                    no_cost_plan = optimization.execution_plan.get_no_cost_plan()
                    for plan_line in plan_heatmap.values():
                        for optimization_line in no_cost_plan.split("->"):
                            if SequenceMatcher(
                                    a=optimization.execution_plan.get_no_tree_plan_str(
                                        plan_line['str']),
                                    b=optimization.execution_plan.get_no_tree_plan_str(
                                        optimization_line)
                            ).ratio() > 0.9:
                                plan_line['weight'] += 1

        self.execution_plan_heatmap = plan_heatmap

        return plan_heatmap

    def get_best_optimization(self, config):
        best_optimization = self
        if best_optimization.optimizations:
            if best_optimization.execution_time_ms < 0:
                best_optimization = best_optimization.optimizations[0]
            for optimization in best_optimization.optimizations:
                if 0 < optimization.execution_time_ms < best_optimization.execution_time_ms:
                    best_optimization = optimization

            if allowed_diff(config, best_optimization.execution_time_ms, self.execution_time_ms):
                return self

        return best_optimization


@dataclasses.dataclass
class PostgresOptimization(PostgresQuery, Optimization):
    execution_plan: 'PostgresExecutionPlan' = None

    def get_default_tipped_query(self):
        return f"/*+ {self.optimizer_tips.debug_hints} {self.explain_hints} */ {self.query}"

    def get_query(self):
        return self.get_default_tipped_query()

    def get_explain(self):
        return f"{Config().explain_clause} {self.get_default_tipped_query()}"

    def get_heuristic_explain(self):
        return f"EXPLAIN {self.get_default_tipped_query()}"


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

        if not optimizations and self.leading.table_scan_hints:
            # case w/o any joins
            for table_scan_hint in itertools.product(*self.leading.table_scan_hints):
                explain_hints = f"{' '.join(table_scan_hint)}"

                self.add_optimization(explain_hints, optimizations)

        return optimizations

    def add_optimization(self, explain_hints, optimizations):
        skip_optimization = self.filter_optimization_tips(explain_hints)
        if not skip_optimization:
            optimizations.append(
                PostgresOptimization(
                    query=self.query.query,
                    query_hash=self.query.query_hash,
                    optimizer_tips=self.query.optimizer_tips,
                    explain_hints=explain_hints
                )
            )


class PostgresCollectResult(CollectResult):
    queries: List[PostgresQuery] = None


class PostgresResultsLoader(ResultsLoader):

    def __init__(self):
        super().__init__()
        self.clazz = PostgresCollectResult
