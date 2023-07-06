import dataclasses
import json
import os
from typing import List, Type

from dacite import Config as DaciteConfig
from dacite import from_dict

from objects import Query


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

    def find_query_by_hash(self, query_hash) -> Type[Query] | None:
        return next(
            (query for query in self.queries if query.query_hash == query_hash),
            None,
        )

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