import dataclasses

from typing import List, Dict, Type

from config import Config


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
    is_index: bool = None
    indexes: List[str] = None

    def copy(self):
        return Field(self.name, self.is_index, self.indexes.copy())


@dataclasses.dataclass
class Table:
    alias: str = None
    name: str = None
    fields: List[Field] = None
    rows: int = 0
    size: int = 0

    def copy(self):
        fields = []
        for field in self.fields:
            fields.append(field.copy())

        return Table(self.alias, self.name, fields, self.size)

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

    execution_plan: 'ExecutionPlan' = None
    execution_time_ms: float = 0
    result_cardinality: int = 0
    result_hash: str = None

    parameters: List = None

    optimizations: List['Query'] = None

    execution_plan_heatmap: Dict[int, Dict[str, str]] = None

    def get_query(self):
        return self.query

    def get_explain(self):
        return f"{Config.explain_clause} {self.query}"

    def get_heuristic_explain(self):
        return f"EXPLAIN {self.query}"

    def get_explain_analyze(self):
        return f"EXPLAIN ANALYZE {self.query}"

    def compare_plans(self, execution_plan: Type['ExecutionPlan']):
        pass

    def heatmap(self):
        pass

    def get_best_optimization(self, config):
        pass

    def __eq__(self, other):
        return self.query_hash == other.query_hash

    def __hash__(self):
        return hash(self.query_hash)


@dataclasses.dataclass
class Optimization(Query):
    pass


class PlanNode:
    def __init__(self):
        self.level: int = 0
        self.node_type: str = None
        self.name: str = None
        self.properties: List[str] = []
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
            s += f'  (cost={self.startup_cost}..{self.total_cost} rows={self.plan_rows} width={self.plan_width})'
        if actual:
            if self.nloops == 0:
                s += '  (never executed)'
            else:
                s += f'  (actual time={self.startup_ms}..{self.total_ms} rows={self.rows} nloops={self.nloops})'
        if properties and len(self.properties) > 0:
            s += str(self.properties)
        return s


class ScanNode(PlanNode):
    def __init__(self):
        super().__init__()
        self.table_name: str = None
        self.table_alias: str = None
        self.index_name: str = None

    def __str__(self):
        s = f'{self.level}:{self.node_type}: '
        s += f'table={self.table_name} alias={self.table_alias} index={self.index_name}'
        return s


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
