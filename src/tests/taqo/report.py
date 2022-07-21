import hashlib
import itertools
import math

from matplotlib import pyplot as plt
from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report


class TaqoReport(Report):
    def __init__(self):
        super().__init__()
        self.same_execution_plan = []
        self.better_plan_found = []

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    def calculate_score(self, optimizations):
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
        except ArithmeticError:
            self.logger.debug("Failed to calculate score, setting TAQO score as 0.0")
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
        plt.savefig(f"report/{self.start_date}/{file_name}")
        plt.close()

        return file_name

    def add_query(self, query: Query):
        best_optimization = query.get_best_optimization()

        if self.allowed_diff(query.execution_time_ms, best_optimization.execution_time_ms):
            self.same_execution_plan.append(query)
        else:
            self.better_plan_found.append(query)

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.report += "\n<<better>>\n"
        self.report += "\n<<found>>\n"

        self.report += "\n[#better]\n== Better plan found queries\n\n"
        for query in self.better_plan_found:
            self.__report_query(query)

        self.report += "\n[#found]\n== No better plan found\n\n"
        for query in self.same_execution_plan:
            self.__report_query(query)

    # noinspection InsecureHash
    def __report_query(self, query: Query):
        best_optimization = query.get_best_optimization()

        self.reported_queries_counter += 1
        query_hash = hashlib.md5(query.query.encode('utf-8')).hexdigest()

        self.report += f"=== Query {query_hash} " \
                       f"(TAQO efficiency - {self.calculate_score(query.optimizations)})"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(query.query)
        self._end_source()

        self._add_double_newline()
        self.report += f"Better optimization hints - `{best_optimization.explain_hints}`"
        self._add_double_newline()

        filename = self.create_plot(best_optimization, query.optimizations, query)
        self.report += f"image::{filename}[\"Query {self.reported_queries_counter}\"]"

        self._add_double_newline()

        self._start_execution_plan_tables()

        self.report += "|Comparison analysis\n"

        self._start_table_row()
        self.report += f"Optimizer cost: `{query.optimizer_score}` (default) vs `{best_optimization.optimizer_score}` (best)"
        self._end_table_row()

        self.report += "\n"

        self._start_table_row()
        self.report += f"Execution time: `{query.execution_time_ms}` (default) vs `{best_optimization.execution_time_ms}` (best)"
        self._end_table_row()

        self._start_table_row()

        if self.config.compare_with_pg:
            self._start_collapsible("Postgres plan")
            self._start_source(["diff"])
            self.report += query.postgres_query.execution_plan
            self._end_source()
            self._end_collapsible()

        self._start_collapsible("Original plan")
        self._start_source(["diff"])
        self.report += query.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Best plan")
        self._start_source(["diff"])
        self.report += best_optimization.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(query.execution_plan, best_optimization.execution_plan)
        if not diff:
            diff = query.execution_plan

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_execution_plan_tables()

        self._add_double_newline()