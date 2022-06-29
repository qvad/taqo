import difflib
import itertools
import math
import os
import shutil
import matplotlib.pyplot as plt


from typing import List
from sql_formatter.core import format_sql
from database import Optimization, Query


class Report:
    def __init__(self):
        self.report = ""
        self.reported_queries_counter = 0

        shutil.rmtree("report")

        os.mkdir("report")
        os.mkdir("report/imgs")

    def __add_double_newline(self):
        self.report += "\n\n"

    def __start_execution_plan_tables(self):
        self.report += "[cols=\"1,1\"]\n|===\n"

    def __start_table_row(self):
        self.report += "a|"

    def __end_table_row(self):
        self.report += "\n"

    def __end_execution_plan_tables(self):
        self.report += "|===\n"

    def __start_source(self, additional_tags=None):
        tags = f",{','.join(additional_tags)}" if additional_tags else ""

        self.report += f"[source{tags}]\n----\n"

    def __end_source(self):
        self.report += "\n----\n"

    def __get_plan_diff(self, original, changed):
        return "\n".join(
            text for text in difflib.unified_diff(original.split("\n"), changed.split("\n")) if
            text[:3] not in ('+++', '---', '@@ '))

    def add_query(self, query: Query, best_optimization: Optimization,
                  optimizations: List[Optimization]):
        self.reported_queries_counter += 1

        self.report += f"= Query {self.reported_queries_counter}, " \
                       f"optimizer score - {self.calculate_score(optimizations)}"
        self.__add_double_newline()

        self.__start_source(["sql"])
        self.report += format_sql(query.query)
        self.__end_source()

        self.__add_double_newline()
        self.report += f"Better optimization hints - `+{best_optimization.explain_hints}+`"
        self.__add_double_newline()

        filename = self.create_plot(best_optimization, optimizations, query)
        self.report += f"image::{filename}[\"Query {self.reported_queries_counter}\"]"

        self.__start_execution_plan_tables()

        self.report += "|Original |Optimization\n"

        self.__start_table_row()
        self.report += f"`+{query.optimizer_score}+` optimizer score"
        self.__end_table_row()

        self.__start_table_row()
        self.report += f"`+{best_optimization.optimizer_score}+` optimizer score"
        self.__end_table_row()

        self.__start_table_row()
        self.__start_source()
        self.report += query.execution_plan
        self.__end_source()
        self.__end_table_row()

        self.__start_table_row()
        self.__start_source(["diff"])
        self.report += self.__get_plan_diff(query.execution_plan, best_optimization.execution_plan)
        self.__end_source()
        self.__end_table_row()

        self.report += "\n"

        self.__start_table_row()
        self.report += f"`+{query.execution_time_ms}ms+` execution time"
        self.__end_table_row()

        self.__start_table_row()
        self.report += f"`+{best_optimization.execution_time_ms}ms+` execution time"
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

        a_best = max(op.execution_time_ms for op in optimizations)
        e_best = max(op.optimizer_score for op in optimizations)

        a_diff = a_best - min(op.execution_time_ms for op in optimizations)
        e_diff = e_best - min(op.optimizer_score for op in optimizations)

        score = 0
        # TODO check score calculation
        for pi, pj in list(itertools.combinations(optimizations, 2)):
            score += (pi.execution_time_ms / a_best) *\
                     (pj.execution_time_ms / a_best) *\
                     math.sqrt(((pj.execution_time_ms - pi.execution_time_ms) / a_diff) ** 2 +
                               ((pj.optimizer_score - pi.optimizer_score) / e_diff) ** 2) *\
                     math.copysign(1, (pj.optimizer_score - pi.optimizer_score))

        return "{:.2f}".format(score)

    def create_plot(self, best_optimization, optimizations, query):
        plt.xlabel('Execution time')
        plt.ylabel('Optimizer score')

        plt.title('Red - optimizations, Blue - original query, Green - best optimization')

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms != 0],
                 [q.optimizer_score for q in optimizations if q.execution_time_ms != 0], 'r^',
                 [query.execution_time_ms],
                 [query.optimizer_score], 'bs',
                 [best_optimization.execution_time_ms],
                 [best_optimization.optimizer_score], 'go')

        file_name = f'imgs/query_{self.reported_queries_counter}.png'
        plt.savefig(f"report/{file_name}")
        plt.close()

        return file_name

    def publish_report(self):
        with open("report/taqo.adoc", "w") as file:
            file.write(self.report)
