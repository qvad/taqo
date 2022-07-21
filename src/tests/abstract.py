import abc
import difflib
import os
import shutil
import subprocess
import time
from abc import ABC
from pathlib import Path

from config import Config
from db.yugabyte import factory


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
        self.yugabyte.stop_database()
        self.yugabyte.destroy()
        self.yugabyte.start_database()

    def stop_db(self):
        self.yugabyte.stop_database()


class Report:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

        self.report = f"= Query Optimizer Test report \n" \
                      f":source-highlighter: coderay\n" \
                      f":coderay-linenums-mode: inline\n\n"

        self._start_source()
        self.report += str(self.config)
        self._end_source()

        self.reported_queries_counter = 0
        self.queries = []

        self.start_date = time.strftime("%Y%m%d-%H%M%S")

        if self.config.clear:
            self.logger.info("Clearing report directory")
            shutil.rmtree("report", ignore_errors=True)

        if not os.path.isdir("report"):
            os.mkdir("report")

        os.mkdir(f"report/{self.start_date}")
        os.mkdir(f"report/{self.start_date}/imgs")

        self.logger.info(f"Created report folder for this run at 'report/{self.start_date}'")

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
        report_adoc = f"report/{self.start_date}/report_{report_name}.adoc"

        with open(report_adoc, "w") as file:
            file.write(self.report)

        self.logger.info(f"Generating report file from {report_adoc} and compiling html")
        subprocess.run(
            f'{self.config.asciidoctor_path} '
            f'-a stylesheet={os.path.abspath("css/adoc.css")} '
            f'{report_adoc}',
            shell=True)

        report_html_path = Path(f'report/{self.start_date}/report_{report_name}.html')
        self.logger.info(f"Done! Check report at {report_html_path.absolute()}")

    def allowed_diff(self, original_execution_time, optimization_execution_time):
        if optimization_execution_time <= 0:
            return False

        return (abs(original_execution_time - optimization_execution_time) / optimization_execution_time) < \
               self.config.skip_percentage_delta
