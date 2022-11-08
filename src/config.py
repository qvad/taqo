import dataclasses
import logging
import sys
from enum import Enum
from typing import List, Set


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


class DDLStep(Enum):
    DATABASE = 0
    CREATE = 1
    ANALYZE = 2
    IMPORT = 3
    DROP = 4


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

    ddl_prefix: str = ""
    with_optimizations: bool = False
    source_path: str = None
    output: str = None
    revision: str = None

    num_nodes: int = None
    tserver_flags: List[str] = None
    master_flags: List[str] = None

    connection: ConnectionConfig = None

    compare_with_pg: bool = False
    enable_statistics: bool = False
    explain_clause: bool = False
    session_props: List[str] = None

    test: str = None
    model: str = None
    basic_multiplier: int = None

    random_seed: int = None
    ddls: Set[DDLStep] = None
    clean_db: bool = None
    allow_destroy_db: bool = None
    clean_build: bool = None
    skip_percentage_delta: bool = None
    look_near_best_plan: bool = None

    num_queries: int = None
    parametrized: bool = False
    num_retries: int = None
    num_warmup: int = None
    skip_timeout_delta: int = None
    all_pairs_threshold: int = None

    asciidoctor_path: str = None
    clear: bool = False

    def __str__(self):
        build_param_skipped = "(skipped)" if self.source_path else ""

        connections = f"  Connection - {self.connection}\n"

        return f"{connections}" + \
               f"  Using following explain syntax - '{self.explain_clause} /*+ ... */ QUERY'\n" + \
               f"  Running '{self.test}' test on model '{self.model}'\n" + \
               f"  Repository code path '{self.source_path}', revisions to test {self.revision}\n" + \
               f"  Additional properties defined:\n" + \
               f"    --num_nodes: {self.num_nodes}\n" + \
               f"    --tserver_flags: {self.tserver_flags} {build_param_skipped}\n" + \
               f"    --master_flags: {self.master_flags} {build_param_skipped}\n" + \
               f"    --num_queries: {self.num_queries}\n" + \
               f"    --num_retries: {self.num_retries}\n" + \
               f"    --random_seed: {self.random_seed}\n" + \
               f"    --basic_multiplier: x{self.basic_multiplier}\n" + \
               f"    --skip_timeout_delta: ±{self.skip_timeout_delta}s\n" + \
               f"    --model_creation: {[m.name for m in self.ddls]}\n" + \
               f"    --output: {self.output}.json\n" + \
               f"    --look_near_best_plan: {self.look_near_best_plan}\n" + \
               f"    --all_pairs_threshold: {self.all_pairs_threshold}\n" + \
               f"    --clean_db: {self.clean_db}\n" + \
               f"    --allow_destroy_db: {self.allow_destroy_db}\n" + \
               f"    --clean_build: {self.clean_build}\n" + \
               f"    --asciidoctor_path: '{self.asciidoctor_path}'\n" + \
               f"    --clear: '{self.clear}'\n"
