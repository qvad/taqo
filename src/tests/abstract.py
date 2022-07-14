import abc
import difflib
import os
import shutil
import subprocess
from abc import ABC
from pathlib import Path

import psycopg2

from config import Config
from yugabyte import factory


class AbstractTest(ABC):
    def __init__(self):
        self.yugabyte = None
        self.config = Config()
        self.logger = self.config.logger

    @abc.abstractmethod
    def evaluate(self):
        pass

    def start_db(self):
        self.logger.info("Starting Yugabyte DB")

        self.yugabyte = factory(self.config)
        self.yugabyte.change_version_and_compile(self.config.revisions_or_paths[0])
        self.yugabyte.stop_node()
        self.yugabyte.destroy()
        self.yugabyte.start_node()

    def stop_db(self):
        self.yugabyte.stop_node()

    def connect_to_db(self):
        conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.username,
            password=self.config.password)
        conn.autocommit = True

        return conn


class Report:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

        self.report = f"= Query Optimizer Test report \n" \
                      f":source-highlighter: coderay\n" \
                      f":coderay-linenums-mode: inline\n\n"
        self.reported_queries_counter = 0
        self.queries = []

        shutil.rmtree("report", ignore_errors=True)

        os.mkdir("report")
        os.mkdir("report/imgs")

    def _add_double_newline(self):
        self.report += "\n\n"

    def _start_execution_plan_tables(self):
        self.report += "[cols=\"1\"]\n|===\n"

    def _start_table_row(self):
        self.report += "a|"

    def _end_table_row(self):
        self.report += "\n"

    def _end_execution_plan_tables(self):
        self.report += "|===\n"

    def _start_source(self, additional_tags=None):
        tags = f",{','.join(additional_tags)}" if additional_tags else ""

        self.report += f"[source{tags},linenums]\n----\n"

    def _end_source(self):
        self.report += "\n----\n"

    def _start_collapsible(self, name):
        self.report += f"""\n\n.{name}\n[%collapsible]\n====\n"""

    def _end_collapsible(self):
        self.report += """\n====\n\n"""

    @staticmethod
    def _get_plan_diff(original, changed):
        return "\n".join(
            text for text in difflib.unified_diff(original.split("\n"), changed.split("\n")) if
            text[:3] not in ('+++', '---', '@@ '))

    def publish_report(self, report_name):
        with open(f"report/taqo_{report_name}.adoc", "w") as file:
            file.write(self.report)

        self.logger.info(f"Generating report file from report/taqo_{report_name}.adoc")
        subprocess.run(
            f'{self.config.asciidoctor_path} -a stylesheet={os.path.abspath("css/adoc.css")} report/taqo_{report_name}.adoc',
            shell=True)

        full_path = Path(f'report/taqo_{report_name}.html')
        self.logger.info(f"Done! Check report at {full_path.absolute()}")
