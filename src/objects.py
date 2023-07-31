import dataclasses
from enum import Enum

from typing import List, Dict, Type

from config import Config
from db.abstract import PlanNodeAccessor

EXPLAIN = "EXPLAIN"


class ExplainFlags(Enum):
    ANALYZE = "ANALYZE"
    DIST = "DIST"
    VERBOSE = "VERBOSE"
    TIMING = "TIMING"

    COSTS_OFF = "COSTS OFF"


@dataclasses.dataclass
class FieldInTableHelper:
    table_name: str
    field_name: str

    def copy(self):
        return FieldInTableHelper(self.table_name, self.field_name)

    def __hash__(self):
        return hash(f"{self.table_name}.{self.field_name}")


@dataclasses.dataclass
class Field:
    name: str = None
    position: int = None
    is_index: bool = None
    indexes: List[str] = None
    defined_width: int = None
    avg_width: int = None

    def copy(self):
        return Field(self.name, self.position, self.is_index, self.indexes.copy(),
                     self.defined_width, self.avg_width)


@dataclasses.dataclass
class Table:
    alias: str = None
    name: str = None
    fields: List[Field] = None
    rows: int = 0
    size: int = 0

    def copy(self):
        fields = [field.copy() for field in self.fields]
        return Table(self.alias, self.name, fields, self.rows, self.size)

    def __hash__(self):
        return hash(f"{self.alias}.{self.name}")


@dataclasses.dataclass
class QueryTips:
    accept: List[str] = dataclasses.field(default_factory=list)
    reject: List[str] = dataclasses.field(default_factory=list)
    tags: List[str] = dataclasses.field(default_factory=list)
    max_timeout: str = dataclasses.field(default_factory=str)
    debug_hints: str = dataclasses.field(default_factory=str)


@dataclasses.dataclass
class Query:
    tag: str = ""
    query: str = ""
    query_hash: str = ""
    tables: List[Table] = None

    optimizer_tips: QueryTips = dataclasses.field(default_factory=QueryTips)
    explain_hints: str = ""

    # internal field to detect duplicates
    cost_off_explain: 'ExecutionPlan' = None

    execution_plan: 'ExecutionPlan' = None
    execution_time_ms: float = 0
    result_cardinality: int = 0
    result_hash: str = None

    parameters: List = None

    optimizations: List['Query'] = None

    execution_plan_heatmap: Dict[int, Dict[str, str]] = None

    def get_query(self):
        return self.query

    def get_explain(self, explain_clause: str = None, options: List[ExplainFlags] = None):
        if not explain_clause:
            explain_clause = Config().explain_clause

        options_clause = f" ({', '.join([opt.value for opt in options])})" if options else ""

        return f"{explain_clause}{options_clause} {self.query}"

    def compare_plans(self, execution_plan: Type['ExecutionPlan']):
        pass

    def heatmap(self):
        pass

    def get_best_optimization(self, config):
        pass

    def get_reportable_query(self):
        pass

    def __eq__(self, other):
        return self.query_hash == other.query_hash

    def __hash__(self):
        return hash(self.query_hash)


@dataclasses.dataclass
class Optimization(Query):
    pass


class PlanNode:
    def __init__(self, accessor: PlanNodeAccessor, node_type):
        self.acc = accessor
        self.node_type: str = node_type
        self.level: int = 0
        self.name: str = None
        self.properties = {}
        self.child_nodes: List[PlanNode] = []

        self.startup_cost: float = 0.0
        self.total_cost: float = 0.0
        self.plan_rows: float = 0.0
        self.plan_width: int = 0

        self.startup_ms: float = 0
        self.total_ms: float = 0
        self.rows: float = 0.0
        self.nloops: float = 0.0

    def __cmp__(self, other):
        pass  # todo

    def __str__(self):
        return f'{self.level}:{self.node_type}'

    def get_full_str(self, estimate=True, actual=True, properties=False):
        s = str(self)
        if estimate:
            s += f'  (cost={self.startup_cost}..{self.total_cost} rows={self.plan_rows}' \
                ' width={self.plan_width})'
        if actual:
            if self.nloops == 0:
                s += '  (never executed)'
            else:
                s += f'  (actual time={self.startup_ms}..{self.total_ms} rows={self.rows}' \
                    ' nloops={self.nloops})'
        if properties and len(self.properties) > 0:
            s += str(self.properties)
        return s

    def has_valid_cost(self):
        return self.acc.has_valid_cost(self)


class ScanNode(PlanNode):
    def __init__(self, accessor, node_type, table_name, table_alias, index_name, is_backward):
        super().__init__(accessor, node_type)
        self.table_name: str = table_name
        self.table_alias: str = table_alias
        self.index_name: str = index_name
        self.is_backward: bool = is_backward

        self.is_seq_scan = self.acc.is_seq_scan(self)
        self.is_index_scan = self.acc.is_index_scan(self)
        self.is_index_only_scan = self.acc.is_index_only_scan(self)
        self.is_any_index_scan = self.is_index_scan or self.is_index_only_scan

    def __str__(self):
        s = f'{self.level}:{self.node_type}: '
        s += f'table={self.table_name} alias={self.table_alias} index={self.index_name}'
        return s

    def get_search_condition_str(self, with_label=False):
        return ('  ' if with_label else ' AND ').join(
            filter(lambda cond: cond,
                   [self.get_index_cond(with_label),
                    self.get_remote_filter(with_label),
                    self.get_remote_tfbr_filter(with_label),
                    self.get_local_filter(with_label),
                    ]))

    def get_property(self, key, with_label=False):
        value = self.properties.get(key)
        return value if not with_label else f'{key}: {value}' if value else ''

    def get_index_cond(self, with_label=False):
        return self.acc.get_index_cond(self, with_label)

    def may_have_table_fetch_by_rowid(self):
        return self.acc.may_have_table_fetch_by_rowid(self)

    def get_remote_filter(self, with_label=False):
        return self.acc.get_remote_filter(self, with_label)

    # TFBR: Table Fetch By Rowid
    def get_remote_tfbr_filter(self, with_label=False):
        return self.acc.get_remote_tfbr_filter(self, with_label)

    def get_local_filter(self, with_label=False):
        return self.acc.get_local_filter(self, with_label)

    def get_rows_removed_by_recheck(self, with_label=False):
        return self.acc.get_rows_removed_by_recheck(self, with_label)

    def has_no_filter(self):
        return (not self.get_remote_filter()
                and not self.get_remote_tfbr_filter()
                and not self.get_local_filter()
                and not self.get_rows_removed_by_recheck())

    def get_actual_row_adjusted_cost(self):
        return ((float(self.total_cost) - float(self.startup_cost))
                * float(self.rows) / float(self.plan_rows)
                + float(self.startup_cost))


class PlanNodeVisitor:
    def visit(self, node):
        method = f'visit_{node.__class__.__name__}'
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        for child in node.child_nodes:
            self.visit(child)


class PlanPrinter(PlanNodeVisitor):
    def __init__(self, estimate=True, actual=True):
        super().__init__()
        self.plan_tree_str: str = ""
        self.estimate = estimate
        self.actual = actual

    def visit(self, node):
        for _ in range(node.level):
            self.plan_tree_str += '  '
        self.plan_tree_str += node.get_full_str(self.estimate, self.actual)
        self.plan_tree_str += '\n'
        self.generic_visit(node)

    @staticmethod
    def build_plan_tree_str(node, estimate=True, actual=True):
        printer = PlanPrinter(estimate, actual)
        printer.visit(node)
        return printer.plan_tree_str


@dataclasses.dataclass
class ExecutionPlan:
    full_str: str

    def get_estimated_cost(self):
        pass

    def get_clean_plan(self, execution_plan=None):
        # todo get plan tree instead here to support plan comparison between DBs
        pass


@dataclasses.dataclass
class ListOfOptimizations:
    query = None

    def __init__(self, config: Config, query: Query):
        self.config = config
        self.query = query

    def get_all_optimizations(self):
        pass

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
