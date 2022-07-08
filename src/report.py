import difflib
import hashlib
import itertools
import math
import os
import shutil
import subprocess
import matplotlib.pyplot as plt

from typing import List
from sql_formatter.core import format_sql

from src.config import Config
from src.database import Optimization, Query


# noinspection InsecureHash
class Report:
    def __init__(self, version):
        self.report = f"= Query Optimizer Test report \n" \
                      f":source-highlighter: coderay\n" \
                      f":coderay-linenums-mode: inline\n\n" \
                      f"[VERSION]\n====\n{version}\n====\n\n"
        self.reported_queries_counter = 0

        shutil.rmtree("report", ignore_errors=True)

        os.mkdir("report")
        os.mkdir("report/imgs")

    def __add_double_newline(self):
        self.report += "\n\n"

    def __start_execution_plan_tables(self):
        self.report += "[cols=\"1\"]\n|===\n"

    def __start_table_row(self):
        self.report += "a|"

    def __end_table_row(self):
        self.report += "\n"

    def __end_execution_plan_tables(self):
        self.report += "|===\n"

    def __start_source(self, additional_tags=None):
        tags = f",{','.join(additional_tags)}" if additional_tags else ""

        self.report += f"[source{tags},linenums]\n----\n"

    def __end_source(self):
        self.report += "\n----\n"

    def __start_collapsible(self, name):
        self.report += f"""\n\n.{name}\n[%collapsible]\n====\n"""

    def __end_collapsible(self):
        self.report += """\n====\n\n"""

    @staticmethod
    def __get_plan_diff(original, changed):
        return "\n".join(
            text for text in difflib.unified_diff(original.split("\n"), changed.split("\n")) if
            text[:3] not in ('+++', '---', '@@ '))

    def add_regression_query(self, first_query: Query, second_query: Query):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(first_query.query.encode('utf-8')).hexdigest()

        self.report += f"== Query {query_hash} "
        self.__add_double_newline()

        self.__start_source(["sql"])
        self.report += format_sql(first_query.query)
        self.__end_source()

        self.__add_double_newline()

        self.__start_execution_plan_tables()

        self.report += "|Comparison analysis\n"

        self.__start_table_row()
        self.report += f"`Cost: {first_query.optimizer_score}` (first) vs `{second_query.optimizer_score}` (second)"
        self.__end_table_row()

        self.report += "\n"

        self.__start_table_row()
        self.report += f"`Execution time: {first_query.execution_time_ms}` (first) vs `{second_query.execution_time_ms}` (second)"
        self.__end_table_row()

        self.__start_table_row()

        self.__start_collapsible("First version plan")
        self.__start_source(["diff"])
        self.report += first_query.execution_plan
        self.__end_source()
        self.__end_collapsible()

        self.__start_collapsible("Second version plan")
        self.__start_source(["diff"])
        self.report += second_query.execution_plan
        self.__end_source()
        self.__end_collapsible()

        self.__start_source(["diff"])

        diff = self.__get_plan_diff(first_query.execution_plan, second_query.execution_plan)
        if not diff:
            # todo content identical
            diff = first_query.execution_plan

        self.report += diff
        self.__end_source()
        self.__end_table_row()

        self.report += "\n"

        self.__end_execution_plan_tables()

        self.__add_double_newline()

    def add_taqo_query(self, query: Query, best_optimization: Optimization,
                       optimizations: List[Optimization]):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(query.query.encode('utf-8')).hexdigest()

        self.report += f"== Query {query_hash} " \
                       f"(TAQO efficiency - {self.calculate_score(optimizations)})"
        self.__add_double_newline()

        self.__start_source(["sql"])
        self.report += format_sql(query.query)
        self.__end_source()

        self.__add_double_newline()
        self.report += f"Better optimization hints - `{best_optimization.explain_hints}`"
        self.__add_double_newline()

        filename = self.create_plot(best_optimization, optimizations, query)
        self.report += f"image::{filename}[\"Query {self.reported_queries_counter}\"]"

        self.__add_double_newline()

        self.__start_execution_plan_tables()

        self.report += "|Comparison analysis\n"

        self.__start_table_row()
        self.report += f"Optimizer cost: `{query.optimizer_score}` (default) vs `{best_optimization.optimizer_score}` (best)"
        self.__end_table_row()

        self.report += "\n"

        self.__start_table_row()
        self.report += f"Execution time: `{query.execution_time_ms}` (default) vs `{best_optimization.execution_time_ms}` (best)"
        self.__end_table_row()

        self.__start_table_row()

        self.__start_collapsible("Original plan")
        self.__start_source(["diff"])
        self.report += query.execution_plan
        self.__end_source()
        self.__end_collapsible()

        self.__start_collapsible("Best plan")
        self.__start_source(["diff"])
        self.report += best_optimization.execution_plan
        self.__end_source()
        self.__end_collapsible()

        self.__start_source(["diff"])

        diff = self.__get_plan_diff(query.execution_plan, best_optimization.execution_plan)
        if not diff:
            # todo content identical
            diff = query.execution_plan

        self.report += diff
        self.__end_source()
        self.__end_table_row()

        self.report += "\n"

        self.__end_execution_plan_tables()

        self.__add_double_newline()

    @staticmethod
    def calculate_score(optimizations):
        optimizations = [q for q in optimizations
                         if q and
                         q.optimizer_score is not None
                         and q.execution_time_ms != 0]

        try:
            a_best = max(op.execution_time_ms for op in optimizations)
            e_best = max(op.optimizer_score for op in optimizations)

            a_diff = a_best - min(op.execution_time_ms for op in optimizations)
            e_diff = e_best - min(op.optimizer_score for op in optimizations)

            score = sum(
                (pi.execution_time_ms / a_best) *
                (pj.execution_time_ms / a_best) *
                math.sqrt(((pj.execution_time_ms - pi.execution_time_ms) / a_diff) ** 2 +
                          ((pj.optimizer_score - pi.optimizer_score) / e_diff) ** 2) *
                math.copysign(1, (pj.optimizer_score - pi.optimizer_score))
                for pi, pj in list(itertools.combinations(optimizations, 2)))
        except Exception:
            print("Failed to calculate score, setting TAQO score as 0.0")
            return 0.0

        return "{:.2f}".format(score)

    def create_plot(self, best_optimization, optimizations, query):
        plt.xlabel('Execution time')
        plt.ylabel('Optimizer cost')

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms != 0],
                 [q.optimizer_score for q in optimizations if q.execution_time_ms != 0], 'k.',
                 [query.execution_time_ms],
                 [query.optimizer_score], 'r^',
                 [best_optimization.execution_time_ms],
                 [best_optimization.optimizer_score], 'go')

        file_name = f'imgs/query_{self.reported_queries_counter}.png'
        plt.savefig(f"report/{file_name}")
        plt.close()

        return file_name

    def publish_report(self, report_name):
        with open(f"report/taqo_{report_name}.adoc", "w") as file:
            file.write(self.report)

        print("Generating report file")
        subprocess.run(
            f'{Config().asciidoctor_path} -a stylesheet={os.path.abspath("css/adoc.css")} report/taqo_{report_name}.adoc',
            shell=True)


if __name__ == "__main__":
    print("Generating report file")
    css_link = os.path.abspath("css/adoc.css")
    subprocess.call(f'asciidoctor -a stylesheet={css_link} report/taqo.adoc', shell=True)
