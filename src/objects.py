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
    level: int
    name: str
    properties: List[str]
    children: List['PlanNode']

    total_cost: float
    plan_rows: float
    plan_width: int
    startup_ms: float
    total_ms: float
    rows: float
    nloops: float

    def __init__(self):
        self.level = 0
        self.name = ""
        self.properties = []
        self.children = []

        self.startup_cost = 0.0
        self.total_cost = 0.0
        self.plan_rows = 0.0
        self.plan_width = 0
        self.startup_ms = 0
        self.total_ms = 0
        self.rows = 0.0
        self.nloops = 0.0

    def __cmp__(self, other):
        pass  # todo

    def __str__(self):
        return self.get_full_str(estimate=False, actual=False, properties=False)

    def get_full_str(self, estimate=True, actual=True, properties=False):
        s = f'{self.level}: {self.name}'
        if estimate:
            s += f'  (cost={self.startup_cost}..{self.total_cost} rows={self.plan_rows} width={self.plan_width})'
        if actual:
            if self.nloops == 0:
                s += '  (never executed)'
            else:
                s += f'  (actual time={self.startup_ms}..{self.total_ms} rows={self.rows} nloops={self.nloops})'
        if properties and len(self.properties) > 0:
            s += self.properties.__str__()
        return s

    def get_tree_str(self, estimate=True, actual=True, properties=False):
        s = self.get_full_str(estimate, actual, properties, False)
        for child in self.children:
            s += '\n'
            for _ in range(child.level):
                s += '  '
            s += child.get_tree_str(estimate, actual, properties)
        return s


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
