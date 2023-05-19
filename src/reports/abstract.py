import difflib
import os
import shutil
import subprocess
import time
from pathlib import Path

from config import Config


class Report:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

        self.report = f"= Optimizer {self.get_report_name()} Test Report \n" \
                      f":source-highlighter: coderay\n" \
                      f":coderay-linenums-mode: inline\n\n"

        self._start_collapsible("Configuration")
        self._start_source()
        self.report += str(self.config)
        self._end_source()
        self._end_collapsible()

        self.reported_queries_counter = 0
        self.queries = []

        self.start_date = time.strftime("%Y%m%d-%H%M%S")

        if self.config.clear:
            self.logger.info("Clearing report directory")
            shutil.rmtree("report", ignore_errors=True)

        if not os.path.isdir("report"):
            os.mkdir("report")

        if not os.path.isdir(f"report/{self.start_date}"):
            os.mkdir(f"report/{self.start_date}")

    def get_report_name(self):
        return ""

    def report_model(self, model_queries):
        if model_queries:
            self._start_collapsible("Model queries")
            self._start_source(["sql"])
            self.report += "\n".join(
                [query if query.endswith(";") else f"{query};" for query in model_queries])
            self._end_source()
            self._end_collapsible()

    def _add_double_newline(self):
        self.report += "\n\n"

    def _start_table(self, columns: str = "1"):
        self.report += f"[cols=\"{columns}\"]\n" \
                       "|===\n"

    def _start_table_row(self):
        self.report += "a|"

    def _end_table_row(self):
        self.report += "\n"

    def _end_table(self):
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
    def _get_plan_diff(baseline, changed):
        return "\n".join(
            text for text in difflib.unified_diff(baseline.split("\n"), changed.split("\n")) if
            text[:3] not in ('+++', '---', '@@ '))

    def publish_report(self, report_name):
        report_adoc = f"report/{self.start_date}/report_{report_name}_{self.config.output}.adoc"

        with open(report_adoc, "w") as file:
            file.write(self.report)

        self.logger.info(f"Generating report file from {report_adoc} and compiling html")
        subprocess.run(
            f'{self.config.asciidoctor_path} '
            f'-a stylesheet={os.path.abspath("css/adoc.css")} '
            f'{report_adoc}',
            shell=True)

        report_html_path = Path(f'report/{self.start_date}/report_{report_name}_{self.config.output}.html')
        self.logger.info(f"Done! Check report at {report_html_path.absolute()}")
