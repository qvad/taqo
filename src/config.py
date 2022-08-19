import dataclasses
import logging
import sys
from typing import List


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        # sourcery skip: instance-method-first-arg-name
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def init_logger(level="INFO") -> logging.Logger:
    f = logging.Formatter('%(asctime)s:%(levelname)5s: %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(f)

    _logger = logging.getLogger("taqo")
    _logger.setLevel(level)
    _logger.addHandler(console_handler)

    return _logger


@dataclasses.dataclass
class ConnectionConfig:
    host: str = None
    port: str = None
    username: str = None
    password: str = None
    database: str = None

    def __str__(self):
        return f"{self.host}:{self.port}@{self.username}:{self.password}, database '{self.database}'"


@dataclasses.dataclass
class Config(metaclass=Singleton):
    logger: logging.Logger = None

    yugabyte_code_path: str = None
    previous_results_path: str = None
    revisions_or_paths: List[str] = None

    num_nodes: int = None
    tserver_flags: List[str] = None
    master_flags: List[str] = None

    postgres: ConnectionConfig = None
    yugabyte: ConnectionConfig = None

    compare_with_pg: bool = False
    enable_statistics: bool = False

    test: str = None
    model: str = None
    basic_multiplier: int = None

    random_seed: int = None
    use_allpairs: bool = None
    skip_table_scan_hints: bool = None
    skip_model_creation: bool = None
    skip_percentage_delta: bool = None
    look_near_best_plan: bool = None

    num_queries: int = None
    num_retries: int = None
    num_warmup: int = None
    skip_timeout_delta: int = None
    max_optimizations: int = None

    asciidoctor_path: str = None
    clear: bool = False

    def __str__(self):
        explain_query = "EXPLAIN ANALYZE" if self.enable_statistics else "EXPLAIN"

        build_param_skipped = "(skipped)" if self.yugabyte_code_path else ""

        connections = f"  Yugabyte Connection - {self.yugabyte}\n"
        if self.compare_with_pg:
            connections += f"  Postgres Connection - {self.postgres}\n"

        return f"{connections}" + \
               f"  Using following explain syntax - '{explain_query} /*+ ... */ QUERY'\n" + \
               f"  Running '{self.test}' test on model '{self.model}'\n" + \
               f"  Repository code path '{self.yugabyte_code_path}', revisions to test {self.revisions_or_paths}\n" + \
               f"  Additional properties defined:\n" + \
               f"    --previous_results_path: {self.previous_results_path}\n" + \
               f"    --num_nodes: {self.num_nodes} {build_param_skipped}\n" + \
               f"    --tserver_flags: {self.tserver_flags} {build_param_skipped}\n" + \
               f"    --master_flags: {self.master_flags} {build_param_skipped}\n" + \
               f"    --num_queries: {self.num_queries}\n" + \
               f"    --num_retries: {self.num_retries}\n" + \
               f"    --use_allpairs: {self.use_allpairs}\n" + \
               f"    --random_seed: {self.random_seed}\n" + \
               f"    --basic_multiplier: x{self.basic_multiplier}\n" + \
               f"    --skip_timeout_delta: ±{self.skip_timeout_delta}s\n" + \
               f"    --skip_table_scan_hints: {self.skip_table_scan_hints}\n" + \
               f"    --skip_model_creation: {self.skip_model_creation}\n" + \
               f"    --look_near_best_plan: {self.look_near_best_plan}\n" + \
               f"    --max_optimizations: {self.max_optimizations}\n" + \
               f"    --asciidoctor_path: '{self.asciidoctor_path}'\n" + \
               f"    --clear: '{self.clear}'\n"
