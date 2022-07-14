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


def init_logger() -> logging.Logger:
    f = logging.Formatter('%(asctime)s:%(levelname)5s: %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(f)

    _logger = logging.getLogger("taqo")
    _logger.setLevel("INFO")
    _logger.addHandler(console_handler)

    return _logger


@dataclasses.dataclass
class Config(metaclass=Singleton):
    logger: logging.Logger = init_logger()

    host: str = None
    port: str = None
    username: str = None
    password: str = None
    database: str = None

    enable_statistics: bool = False

    test: str = None
    model: str = None

    yugabyte_code_path: str = None
    revisions_or_paths: List[str] = None

    skip_table_scan_hints: bool = None
    skip_model_creation: bool = None
    num_queries: int = None
    num_retries: int = None
    skip_timeout_delta: int = None
    max_optimizations: int = None

    asciidoctor_path: str = None

    def __str__(self):
        explain_query = "EXPLAIN ANALYZE" if self.enable_statistics else "EXPLAIN"

        return f"Current test configuration:\n" + \
               f"  Connection - {self.host}:{self.port}@{self.username}:{self.password}, database '{self.database}'\n" + \
               f"  Using following explain syntax - '{explain_query} /*+ ... */ QUERY'\n" + \
               f"  Running '{self.test}' test on model '{self.model}'\n" + \
               f"  Repository code path '{self.yugabyte_code_path}', revisions to test {self.revisions_or_paths}\n" + \
               f"  Additional properties:\n" + \
               f"    - skip_table_scan_hints: {self.skip_table_scan_hints}\n" + \
               f"    - skip_model_creation: {self.skip_model_creation}\n" + \
               f"    - num_queries: {self.num_queries}\n" + \
               f"    - num_retries: {self.num_retries}\n" + \
               f"    - skip_timeout_delta: ±{self.skip_timeout_delta}s\n" + \
               f"    - max_optimizations: {self.max_optimizations}\n" + \
               f"    - asciidoctor_path: '{self.asciidoctor_path}'\n"
