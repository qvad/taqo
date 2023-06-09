import dataclasses
import logging
import sys
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
    DROP = 4


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

    asciidoctor_path: str = None
    clear: bool = False

    def __str__(self):
        return "Configuration" + \
               f"DB - {self.database.__class__.__name__}\n" \
               f"\n" \
               f"remote_data_path - {self.remote_data_path}\n" \
               f"ddl_prefix - {self.ddl_prefix}\n" \
               f"with_optimizations - {self.with_optimizations}\n" \
               f"source_path - {self.source_path}\n" \
               f"output - {self.output}\n" \
               f"\n" \
               f"revision - {self.revision}\n" \
               f"num_nodes - {self.num_nodes}\n" \
               f"tserver_flags - {self.tserver_flags}\n" \
               f"master_flags - {self.master_flags}\n" \
               f"\n" \
               f"(initial) connection - {self.connection}\n" \
               f"enable_statistics - {self.enable_statistics}\n" \
               f"explain_clause - {self.explain_clause}\n" \
               f"session_props - {self.session_props}\n" \
               f"\n" \
               f"test - {self.test}\n" \
               f"model - {self.model}\n" \
               f"basic_multiplier - {self.basic_multiplier}\n" \
               f"ddls - {[m.name for m in self.ddls]}\n" \
               f"clean_db - {self.clean_db}\n" \
               f"allow_destroy_db - {self.allow_destroy_db}\n" \
               f"clean_build - {self.clean_build}\n" \
               f"skip_percentage_delta - {self.skip_percentage_delta}\n" \
               f"look_near_best_plan - {self.look_near_best_plan}\n" \
               f"num_queries - {self.num_queries}\n" \
               f"parametrized - {self.parametrized}\n" \
               f"num_retries - {self.num_retries}\n" \
               f"num_warmup - {self.num_warmup}\n" \
               f"skip_timeout_delta - {self.skip_timeout_delta}\n" \
               f"ddl_query_timeout - {self.ddl_query_timeout}\n" \
               f"test_query_timeout - {self.test_query_timeout}\n" \
               f"all_pairs_threshold - {self.all_pairs_threshold}\n" \
               f"asciidoctor_path - {self.asciidoctor_path}\n" \
               f"clear - {self.clear}\n"
