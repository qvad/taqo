import dataclasses
import json
import os
from typing import List, Dict, Type

from dacite import Config as DaciteConfig
from dacite import from_dict

from config import Config


@dataclasses.dataclass
class Field:
    name: str = None
    is_index: bool = None


@dataclasses.dataclass
class Table:
    alias: str = None
    name: str = None
    fields: List[Field] = None
    size: int = 0


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


@dataclasses.dataclass
class Optimization(Query):
    pass


@dataclasses.dataclass
class CollectResult:
    db_version: str = ""
    git_message: str = ""
    config: str = ""
    model_queries: List[str] = None
    queries: List[Type[Query]] = None

    def append(self, new_element):
        if not self.queries:
            self.queries = [new_element, ]
        else:
            self.queries.append(new_element)

        # CPUs are cheap
        self.queries.sort(key=lambda q: q.query_hash)

    def find_query_by_hash(self, query_hash):
        return next(
            (query for query in self.queries if query.query_hash == query_hash),
            None,
        )


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


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class ResultsLoader:

    def __init__(self):
        self.clazz = CollectResult

    def get_queries_from_previous_result(self, previous_execution_path):
        with open(previous_execution_path, "r") as prev_result:
            return from_dict(self.clazz, json.load(prev_result), DaciteConfig(check_types=False))

    def store_queries_to_file(self, queries: Type[CollectResult], output_json_name: str):
        if not os.path.isdir("report"):
            os.mkdir("report")

        with open(f"report/{output_json_name}.json", "w") as result_file:
            result_file.write(json.dumps(queries, cls=EnhancedJSONEncoder))
