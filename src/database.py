import dataclasses
import json
from typing import List, Dict

from dacite import Config as DaciteConfig
from dacite import from_dict

from config import Config
from db.postgres import Table, QueryTips


@dataclasses.dataclass
class Query:
    tag: str = ""
    query: str = ""
    query_hash: str = ""
    tables: List[Table] = None

    optimizer_tips: QueryTips = None
    explain_hints: str = ""

    execution_plan: 'ExecutionPlan' = None
    execution_time_ms: float = 0
    optimizer_score: float = 1
    result_cardinality: int = 0
    result_hash: str = None

    parameters: List = None

    optimizations: List['Query'] = None

    execution_plan_heatmap: Dict[int, Dict[str, str]] = None

    def get_best_optimization(self, config: Config):
        pass


@dataclasses.dataclass
class ListOfQueries:
    db_version: str = ""
    git_message: str = ""
    model_queries: List[str] = None
    queries: List[Query] = None

    def append(self, new_element):
        if not self.queries:
            self.queries = [new_element, ]
        else:
            self.queries.append(new_element)

        # CPUs are cheap in 2022
        self.queries.sort(key=lambda q: q.query_hash)


class EPNode:
    def __init__(self):
        self.root: 'EPNode' | None = None
        self.childs: List['EPNode'] = []
        self.type: str = ""
        self.full_str: str = ""
        self.level: int = 0

    def __str__(self):
        return self.full_str


@dataclasses.dataclass
class ExecutionPlan:
    full_str: str

    def get_clean_plan(self, execution_plan=None):
        # todo get plan tree instead here to support plan comparison between DBs
        pass

class ListOfOptimizations:
    query = None

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


def store_queries_to_file(queries: ListOfQueries, output_json_name: str):
    with open(f"report/{output_json_name}.json", "w") as result_file:
        result_file.write(json.dumps(queries, cls=EnhancedJSONEncoder))
