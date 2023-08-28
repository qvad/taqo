import dataclasses
import itertools
import re
from difflib import SequenceMatcher
from enum import Enum
from typing import List, Type

import psycopg2
from allpairspy import AllPairs

from collect import CollectResult, ResultsLoader
from config import Config, ConnectionConfig, DDLStep
from objects import Query, ExecutionPlan, ListOfOptimizations, Table, Optimization, ExplainFlags
from objects import AggregateNode, JoinNode, SortNode, PlanNode, ScanNode
from db.abstract import PlanNodeAccessor
from db.database import Database
from utils import evaluate_sql, allowed_diff, parse_clear_and_parametrized_sql

DEFAULT_USERNAME = 'postgres'
DEFAULT_PASSWORD = 'postgres'

ENABLE_PLAN_HINTING = "SET pg_hint_plan.enable_hint = ON;"
ENABLE_DEBUG_HINTING = "SET pg_hint_plan.debug_print = ON;"
CLIENT_MESSAGES_TO_LOG = "SET client_min_messages TO log;"
DEBUG_MESSAGE_LEVEL = "SET pg_hint_plan.message_level = debug;"

PLAN_CLEANUP_REGEX = r"\s\(actual time.*\)|\s\(never executed\)|\s\(cost.*\)|" \
                     r"\sMemory:.*|Planning Time.*|Execution Time.*|Peak Memory Usage.*|" \
                     r"Storage Read Requests:.*|Storage Read Execution Time:.*|Storage Write Requests:.*|" \
                     r"Catalog Read Requests:.*|Catalog Read Execution Time:.*|Catalog Write Requests:.*|" \
                     r"Catalog Reads Requests:.*|Catalog Reads Execution Time:.*|Catalog Writes Requests:.*|" \
                     r"Storage Flushes Requests:.*|Storage Execution Time:.*|" \
                     r"Storage Table Read Requests:.*|Storage Table Read Execution Time:.*|Output:.*|" \
                     r"Storage Index Read Requests:.*|Storage Index Read Execution Time:.*|" \
                     r"Storage Flush Requests:.*|" \
                     r"Disk:.*|" \
                     r"Metric rocksdb_.*:.*|" \
                     r"Read RPC Count:.*|Read RPC Wait Time:.*|DocDB Scanned Rows:.*|" \
                     r".*Partial Aggregate:.*|YB\s|Remote\s|" \
                     r"JIT:.*|\s+Functions:.*|\s+Options:.*|\s+Timing:.*"
PLAN_RPC_CALLS = r"\nRead RPC Count:\s(\d+)"
PLAN_RPC_WAIT_TIMES = r"\nRead RPC Wait Time:\s([+-]?([0-9]*[.])?[0-9]+)"
PLAN_DOCDB_SCANNED_ROWS = r"\nDocDB Scanned Rows:\s(\d+)"
PLAN_PEAK_MEMORY = r"\nPeak memory:\s(\d+)"
PLAN_TREE_CLEANUP = r"\n\s*->\s*|\n\s*"

plan_node_header_pattern = re.compile(''.join([
    r'(?P<name>\S+(?:\s+\S+)*)',
    r'\s+',
    r'\(cost=(?P<sc>\d+\.\d*)\.\.(?P<tc>\d+\.\d*)\s+rows=(?P<prows>\d+)\s+width=(?P<width>\d+)\)',
    r'\s+',
    r'\((?:(?:actual time=(?P<st>\d+\.\d*)\.\.(?P<tt>\d+\.\d*) +rows=(?P<rows>\d+)',
    r' +loops=(?P<loops>\d+))|(?:(?P<never>never executed)))\)',
]))

node_name_decomposition_pattern = re.compile(''.join([
    r'(?P<type>\S+(?:\s+\S+)* Scan)(?P<backward>\s+Backward)*(?: using (?P<index>\S+))*'
    r' on (?:(?P<schema>\S+)\.)*(?P<table>\S+)(?: (?P<alias>\S+))*']))

hash_property_decomposition_pattern = re.compile(''.join([
    r'Buckets: (?P<buckets>\d+)(?: originally (?P<orig_buckets>\d+))*  ',
    r'Batches: (?P<batches>\d+)(?: originally (?P<orig_batches>\d+))*  ',
    r'Memory Usage: (?P<peak_mem>\d+)kB',
]))

PG_DISABLE_COST = 10000000000.00


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

    def set_query_timeout(self, cur, timeout):
        self.logger.debug(f"Setting statement timeout to {timeout} seconds")
        evaluate_sql(cur, f"SET statement_timeout = '{timeout}s'")

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
        elif len(self.tables) == self.config.all_pairs_threshold:
            self.get_all_pairs_with_all_table_permutations()
        else:
            self.get_all_pairs_combinations()

    def get_all_combinations(self):
        # algorithm with all possible combinations
        for tables_perm in itertools.permutations(self.tables):
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

        self.table_scan_hints = itertools.product(*self.get_table_scan_hints())

    def get_all_pairs_with_all_table_permutations(self):
        # algorithm with all possible table permutations
        # but with all pairs scans
        table_permutations = list(itertools.permutations(self.tables))
        join_product = list(AllPairs([list(Joins) for _ in range(len(self.tables) - 1)]))
        scan_product = list(AllPairs(self.get_table_scan_hints()))

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
            scan_hints = " ".join(scans)

            self.joins.append(f"{leading_hint} {query_joins} {scan_hints}")

    def get_all_pairs_combinations(self):
        if len(self.tables) <= 1:
            return

        self.table_scan_hints = list(AllPairs(self.get_table_scan_hints()))

    def get_table_scan_hints(self):
        table_scan_hints = []
        for table in self.tables:
            tables_and_idxs = {f"{Scans.SEQ.value}({table.alias})",
                               f"{Scans.INDEX.value}({table.alias})",
                               f"{Scans.INDEX_ONLY.value}({table.alias})"}

            if self.config.all_index_check:
                indexes = []
                for field in table.fields:
                    if field.is_index:
                        indexes += field.indexes

                tables_and_idxs |= {
                    f"{Scans.INDEX.value}({table.alias} {index})"
                    for index in indexes
                }
                tables_and_idxs |= {
                    f"{Scans.INDEX_ONLY.value}({table.alias} {index})"
                    for index in indexes
                }
            else:
                tables_and_idxs |= {
                    f"{Scans.INDEX.value}({table.alias})"
                    for field in table.fields
                    if field.is_index
                }
                tables_and_idxs |= {
                    f"{Scans.INDEX_ONLY.value}({table.alias})"
                    for field in table.fields
                    if field.is_index
                }

            table_scan_hints.append(list(tables_and_idxs))

        return table_scan_hints


@dataclasses.dataclass
class PostgresQuery(Query):
    execution_plan: 'PostgresExecutionPlan' = None
    optimizations: List['PostgresOptimization'] = None

    def get_debug_hints(self):
        return f"/*+ {self.optimizer_tips.debug_hints} */ " if self.optimizer_tips.debug_hints else ""

    def get_query(self):
        return f"{self.get_debug_hints()}{self.query}"

    def tips_looks_fair(self, optimization):
        clean_plan = self.execution_plan.get_clean_plan()

        return not any(
            join.value[0] in optimization.explain_hints and join.value[1] not in clean_plan for join in Joins)

    def compare_plans(self, execution_plan: Type['ExecutionPlan']):
        if self.execution_plan:
            return self.execution_plan.get_clean_plan() == self.execution_plan.get_clean_plan(execution_plan)
        else:
            return False

    def get_reportable_query(self):
        _, _, sql_wo_parameters = parse_clear_and_parametrized_sql(self.query.replace("|", "\|"))
        return sql_wo_parameters

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
                                    a=optimization.execution_plan.get_no_tree_plan_str(plan_line['str']),
                                    b=optimization.execution_plan.get_no_tree_plan_str(optimization_line)
                            ).ratio() > 0.9:
                                plan_line['weight'] += 1

        self.execution_plan_heatmap = plan_heatmap

        return plan_heatmap

    def get_best_optimization(self, config):
        best_optimization = self
        if best_optimization.optimizations:
            if best_optimization.execution_time_ms < 0:
                best_optimization = best_optimization.optimizations[0]
            for optimization in self.optimizations:
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

    def get_explain(self, explain_clause: str = None, options: List[ExplainFlags] = None):
        if not explain_clause:
            explain_clause = Config().explain_clause

        options_clause = f" ({', '.join([opt.value for opt in options])})" if options else ""

        return f"{explain_clause}{options_clause} {self.get_default_tipped_query()}"


class PostgresPlanNodeAccessor(PlanNodeAccessor):
    @staticmethod
    def has_valid_cost(node):
        return float(node.total_cost) < PG_DISABLE_COST

    @staticmethod
    def fixup_invalid_cost(node):
        from sys import float_info
        from math import log
        scost = float(node.startup_cost)
        tcost = float(node.total_cost)
        if ((scost > 0 and log(scost, 10) >= float_info.mant_dig - 1)
                or (tcost > 0 and log(tcost, 10) >= float_info.mant_dig - 1)):
            return True

        node.startup_cost = round(scost % PG_DISABLE_COST, 3)
        node.total_cost = round(tcost % PG_DISABLE_COST, 3)
        return False

    @staticmethod
    def is_seq_scan(node):
        return node.node_type == 'Seq Scan' or node.node_type == 'YB Seq Scan'

    @staticmethod
    def is_index_scan(node):
        return node.node_type == 'Index Scan'

    @staticmethod
    def is_index_only_scan(node):
        return node.node_type == 'Index Only Scan'

    @staticmethod
    def get_index_cond(node, with_label=False):
        return node.get_property('Index Cond', with_label)

    @staticmethod
    def may_have_table_fetch_by_rowid(node):
        return (PostgresPlanNodeAccessor.is_index_scan(node)
                and not node.index_name.endswith('_pkey'))

    @staticmethod
    def get_remote_filter(node, with_label=False):
        return node.get_property('Remote Index Filter'
                                 if PostgresPlanNodeAccessor.may_have_table_fetch_by_rowid(node)
                                 else 'Remote Filter', with_label)

    @staticmethod
    def get_remote_tfbr_filter(node, with_label=False):
        return (node.get_property('Remote Filter', with_label)
                if node.may_have_table_fetch_by_rowid() else '')

    @staticmethod
    def get_local_filter(node, with_label=False):
        return node.get_property('Filter', with_label)

    @staticmethod
    def get_rows_removed_by_recheck(node, with_label=False):
        return int(node.get_property('Rows Removed by Index Recheck', with_label)
                   or node.get_property('Rows Removed by Recheck', with_label)
                   or 0)

    @staticmethod
    def is_scan_with_partial_aggregate(node):
        return bool(node.get_property('Partial Aggregate'))


@dataclasses.dataclass
class PostgresExecutionPlan(ExecutionPlan):
    __node_accessor = PostgresPlanNodeAccessor()

    def make_node(self, node_name):
        index_name = table_name = table_alias = is_backward = None
        if match := node_name_decomposition_pattern.search(node_name):
            node_type = match.group('type')
            index_name = match.group('index')
            is_backward = match.group('backward') is not None
            table_name = match.group('table')
            table_alias = match.group('alias')
        else:
            node_type = node_name

        if table_name:
            return ScanNode(self.__node_accessor, node_type, table_name, table_alias,
                            index_name, is_backward)

        if 'Join' in node_type or 'Nested Loop' in node_type:
            return JoinNode(self.__node_accessor, node_type)

        if 'Aggregate' in node_type or 'Group' in node_type:
            return AggregateNode(self.__node_accessor, node_type)

        if 'Sort' in node_type:
            return SortNode(self.__node_accessor, node_type)

        return PlanNode(self.__node_accessor, node_type)

    def parse_plan(self):
        node = None
        prev_level = 0
        current_path = []
        for node_str in self.full_str.split('->'):
            node_level = prev_level
            # trailing spaces after the previous newline is the indent of the next node
            node_end = node_str.rfind('\n')
            indent = int(node_str.count(' ', node_end))
            # postgres explain.c adds 6 whitespaces at each indentation level with "  ->  "
            # for each node header. add back 4 for "->  " before division because we split
            # it at each '->'.
            prev_level = int((indent + 4) / 6)

            node_props = (node_str[:node_end].splitlines() if node_str.endswith('\n')
                          else node_str.splitlines())

            if not node_props:
                break

            if match := plan_node_header_pattern.search(node_props[0]):
                node_name = match.group('name')
                node = self.make_node(node_name)

                node.level = node_level
                node.name = node_name
                node.startup_cost = match.group('sc')
                node.total_cost = match.group('tc')
                node.plan_rows = match.group('prows')
                node.plan_width = match.group('width')
                if match.group('never'):
                    node.nloops = 0
                else:
                    node.startup_ms = match.group('st')
                    node.total_ms = match.group('tt')
                    node.rows = match.group('rows')
                    node.nloops = match.group('loops')
            else:
                break

            for prop in node_props[1:]:
                if prop.startswith(' '):
                    prop_str = prop.strip()
                    if match := hash_property_decomposition_pattern.search(prop_str):
                        node.properties['Hash Buckets'] = match.group('buckets')

                        if orig_buckets := match.group('orig_buckets'):
                            node.properties['Original Hash Buckets'] = orig_buckets

                        node.properties['Hash Batches'] = match.group('batches')

                        if orig_batches := match.group('orig_batches'):
                            node.properties['Original Hash Batches'] = orig_batches

                        node.properties['Peak Memory Usage'] = match.group('peak_mem')
                    else:
                        if (keylen := prop_str.find(':')) > 0:
                            node.properties[prop_str[:keylen]] = prop_str[keylen + 1:].strip()

            if not current_path:
                current_path.append(node)
            else:
                while len(current_path) > node.level:
                    current_path.pop()
                current_path[-1].child_nodes.append(node)

        return current_path[0] if current_path else None

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
            return 0
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
            self.add_optimization(leading_join, optimizations)

        if not optimizations and self.leading.table_scan_hints:
            # case w/o any joins
            for table_scan_hint in self.leading.table_scan_hints:
                self.add_optimization(f"{' '.join(table_scan_hint)}", optimizations)

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
