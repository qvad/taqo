import os
import shutil
import subprocess
import time
from pathlib import Path

from config import Config


class ObjectsMixin:
    def __int__(self):
        self.content = ""

    def add_double_newline(self):
        self.content += "\n\n"

    def start_table(self, columns: str = "1"):
        self.content += f"[cols=\"{columns}\"]\n" \
                        "|===\n"

    def start_table_row(self):
        self.content += "a|"

    def end_table_row(self):
        self.content += "\n"

    def end_table(self):
        self.content += "|===\n"

    def start_source(self, additional_tags=None, linenums=True):
        tags = f",{','.join(additional_tags)}" if additional_tags else ""
        tags += ",linenums" if linenums else ""

        self.content += f"[source{tags}]\n----\n"

    def end_source(self):
        self.content += "\n----\n"

    def start_collapsible(self, name, sep='===='):
        self.content += f"""\n\n.{name}\n[%collapsible]\n{sep}\n"""

    def end_collapsible(self, sep='===='):
        self.content += f"""\n{sep}\n\n"""


class AbstractReportAction(ObjectsMixin):
    def __init__(self):
        super().__init__()

        self.config = Config()
        self.logger = self.config.logger

        self.content = f"= Optimizer {self.get_report_name()} Test Report \n" \
                       f":source-highlighter: coderay\n" \
                       f":coderay-linenums-mode: inline\n\n"

        self.start_collapsible("Reporting configuration")
        self.start_source()
        self.content += str(self.config)
        self.end_source()
        self.end_collapsible()

        self.reported_queries_counter = 0
        self.queries = []
        self.sub_reports = []

        self.start_date = time.strftime("%Y%m%d-%H%M%S")

        self.report_folder = f"report/{self.start_date}"
        self.report_folder_imgs = f"report/{self.start_date}/imgs"
        self.report_folder_tags = f"report/{self.start_date}/tags"

        if self.config.clear:
            self.logger.info("Clearing report directory")
            shutil.rmtree("report", ignore_errors=True)

        if not os.path.isdir("report"):
            os.mkdir("report")

        if not os.path.isdir(self.report_folder):
            os.mkdir(self.report_folder)
            os.mkdir(self.report_folder_imgs)
            os.mkdir(self.report_folder_tags)

    def get_report_name(self):
        return ""

    def report_model(self, model_queries):
        if model_queries:
            self.start_collapsible("Model queries")
            self.start_source(["sql"])
            self.content += "\n".join(
                [query if query.endswith(";") else f"{query};" for query in model_queries])
            self.end_source()
            self.end_collapsible()

    def report_config(self, config, collapsible_name):
        if config:
            self.start_collapsible(f"Collect configuration {collapsible_name}")
            self.start_source(["sql"])
            self.content += config
            self.end_source()
            self.end_collapsible()

    def create_sub_report(self, name):
        subreport = SubReport(name)
        self.sub_reports.append(subreport)
        return subreport

    def publish_report(self, report_name):
        index_html = f"{self.report_folder}/index_{self.config.output}.adoc"

        with open(index_html, "w") as file:
            file.write(self.content)

        for sub_report in self.sub_reports:
            with open(f"{self.report_folder_tags}/{sub_report.name}.adoc", "w") as file:
                file.write(sub_report.content)

        self.logger.info(f"Generating report file from {index_html} and compiling html")
        asciidoc_return_code = subprocess.run(
            f'{self.config.asciidoctor_path} '
            f'-a stylesheet={os.path.abspath("css/adoc.css")} '
            f'{index_html}',
            shell=True).returncode

        if self.sub_reports:
            self.logger.info(f"Compiling {len(self.sub_reports)} subreports to html")
            for sub_report in self.sub_reports:
                subprocess.call(
                    f'{self.config.asciidoctor_path} '
                    f'-a stylesheet={os.path.abspath("css/adoc.css")} '
                    f"{self.report_folder_tags}/{sub_report.name}.adoc",
                    shell=True)

        if asciidoc_return_code != 0:
            self.logger.exception("Failed to generate HTML file! Check asciidoctor path")
        else:
            report_html_path = Path(f'{self.report_folder}/index_{self.config.output}.html')
            self.logger.info(f"Done! Check report at {report_html_path.absolute()}")

    def append_tag_page_link(self, subreport_name: str, hashtag: str | None, readable_name: str):
        hashtag = f"#{hashtag}" if hashtag else ""
        self.content += f"\nlink:tags/{subreport_name}.html{hashtag}[{readable_name}]\n"


class SubReport(ObjectsMixin):
    def __init__(self, name):
        self.config = Config()
        self.logger = self.config.logger

        self.name = name
        self.content = f"= {name} subreport \n" \
                       f":source-highlighter: coderay\n" \
                       f":coderay-linenums-mode: inline\n\n"

    def append_index_page_hashtag_link(self, hashtag: str, readable_name: str):
        hashtag = f"#{hashtag}" if hashtag else ""
        self.content += f"\nlink:../index_{self.config.output}.html{hashtag}[{readable_name}]\n"
