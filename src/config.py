import dataclasses
import logging
import sys
import pprint
from copy import copy
from enum import Enum
from typing import List, Set

from db.database import Database


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
    COMPACT = 4
    DROP = 5


@dataclasses.dataclass
class ConnectionConfig:
    host: str = None
    port: str = None
    username: str = None
    password: str = None
    database: str = None

    def __str__(self):
        return f"{self.host}:{self.port}@{self.username}:*******, database '{self.database}'"


@dataclasses.dataclass
class Config(metaclass=Singleton):
    logger: logging.Logger = None

    database: Database = None

    remote_data_path: str = "."
    ddl_prefix: str = ""
    plans_only: bool = False
    with_optimizations: bool = False
    source_path: str = None
    output: str = ""
    revision: str = None

    num_nodes: int = None
    tserver_flags: List[str] = None
    master_flags: List[str] = None

    connection: ConnectionConfig = None

    compare_with_pg: bool = False
    enable_statistics: bool = False
    explain_clause: str = ""
    server_side_execution: bool = False
    session_props: List[str] = None

    test: str = None
    model: str = None
    baseline_path: str = None
    baseline_results: any = None
    all_index_check: bool = None
    load_catalog_tables: bool = None
    basic_multiplier: int = None

    ddls: Set[DDLStep] = None
    clean_db: bool = None
    allow_destroy_db: bool = None
    clean_build: bool = None
    skip_percentage_delta: float = None
    look_near_best_plan: bool = None

    num_queries: int = None
    parametrized: bool = False
    num_retries: int = None
    num_warmup: int = None
    skip_timeout_delta: int = None
    ddl_query_timeout: int = None
    test_query_timeout: int = None
    all_pairs_threshold: int = None

    yugabyte_bin_path: str = None
    yugabyte_master_addresses: str = None
    asciidoctor_path: str = None
    clear: bool = False

    def __str__(self):
        skipped_fields = ['logger', 'database', 'baseline_results']

        self_dict = copy(vars(self))
        for field in skipped_fields:
            self_dict.pop(field)

        self_dict['connection'] = str(self_dict['connection'])
        self_dict['ddls'] = str([m.name for m in self.ddls])

        return str(pprint.pformat(self_dict))
