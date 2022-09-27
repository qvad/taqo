import os

from matplotlib import pyplot as plt
from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report
from utils import allowed_diff, get_md5


class TaqoReport(Report):
    def __init__(self):
        super().__init__()

        os.mkdir(f"report/{self.start_date}")
        os.mkdir(f"report/{self.start_date}/imgs")

        self.logger.info(f"Created report folder for this run at 'report/{self.start_date}'")

        self.failed_validation = []
        self.same_execution_plan = []
        self.better_plan_found = []

    def get_report_name(self):
        return "TAQO"

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    @staticmethod
    def calculate_score(query):
        return "{:.2f}".format(
            query.get_best_optimization().execution_time_ms / query.execution_time_ms)

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

        if self.config.compare_with_pg and query.result_hash != query.postgres_query.result_hash:
            self.failed_validation.append(query)
        if not self.config.compare_with_pg and query.result_hash != best_optimization.result_hash:
            self.failed_validation.append(query)

        if allowed_diff(self.config, query.execution_time_ms, best_optimization.execution_time_ms):
            self.same_execution_plan.append(query)
        else:
            self.better_plan_found.append(query)

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.report += "\n<<result>>\n"
        self.report += "\n<<better>>\n"
        self.report += "\n<<found>>\n"

        self.report += f"\n[#result]\n== Result validation failure ({len(self.failed_validation)})\n\n"
        for query in self.failed_validation:
            self.__report_query(query, True)

        self.report += f"\n[#better]\n== Better plan found queries ({len(self.better_plan_found)})\n\n"
        for query in self.better_plan_found:
            self.__report_query(query, True)

        self.report += f"\n[#found]\n== No better plan found ({len(self.same_execution_plan)})\n\n"
        for query in self.same_execution_plan:
            self.__report_query(query, False)

    def __report_near_queries(self, query: Query):
        best_optimization = query.get_best_optimization()
        if add_to_report := "".join(
                f"`{optimization.explain_hints}`\n\n"
                for optimization in query.optimizations
                if allowed_diff(self.config, best_optimization.execution_time_ms,
                                optimization.execution_time_ms)):
            self._start_collapsible("All best optimization hints")
            self.report += add_to_report
            self._end_collapsible()

    def __report_heatmap(self, query: Query):
        """
        Here is the deal. In PG plans we can separate each plan tree node by splitting by `->`
        When constructing heatmap need to add + or - to the beginning of string `\n`.
        So there is 2 splitters - \n and -> and need to construct correct result.

        :param query:
        :return:
        """
        best_decision = max(row['weight'] for row in query.execution_plan_heatmap.values())
        last_rowid = max(query.execution_plan_heatmap.keys())
        result = ""
        for row_id, row in query.execution_plan_heatmap.items():
            rows = row['str'].split("\n")

            if row['weight'] == best_decision:
                result = self.fix_last_newline_in_result(result, rows)
                result += "\n".join([f"+{line}" for line_id, line in enumerate(rows) if
                                     line_id != (len(rows) - 1)]) + f"\n{rows[-1]}"
            elif row['weight'] == 0:
                result = self.fix_last_newline_in_result(result, rows)
                result += "\n".join([f"-{line}" for line_id, line in enumerate(rows) if
                                     line_id != (len(rows) - 1)]) + f"\n{rows[-1]}"
            else:
                result += f"{row['str']}"

            # skip adding extra -> to the end of list
            if row_id != last_rowid:
                result += "->"

        self._start_collapsible("Plan heatmap")
        self._start_source(["diff"])
        self.report += result
        self._end_source()
        self._end_collapsible()

    @staticmethod
    def fix_last_newline_in_result(result, rows):
        if result:
            splitted_result = result.split("\n")
            result = "\n".join(splitted_result[:-1])
            last_newline = splitted_result[-1]
            rows[0] = f"{last_newline}{rows[0]}"
            result += "\n"
        return result

    # noinspection InsecureHash
    def __report_query(self, query: Query, show_best: bool):
        best_optimization = query.get_best_optimization()

        self.reported_queries_counter += 1
        query_hash = get_md5(query.query)

        self.report += f"=== Query {query_hash} " \
                       f"(Optimizer efficiency - {self.calculate_score(query)})"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(query.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()
        self.report += f"Default explain hints - `{query.explain_hints}`"
        self._add_double_newline()

        if show_best:
            self._add_double_newline()
            self.report += f"Better explain hints - `{best_optimization.explain_hints}`"
            self._add_double_newline()

            self.__report_near_queries(query)

        filename = self.create_plot(best_optimization, query.optimizations, query)
        self.report += f"image::{filename}[\"Query {self.reported_queries_counter}\"]"

        self._add_double_newline()

        self._start_table()

        self.report += "|Comparison analysis\n"

        self._start_table_row()
        if 'order by' in query.query:
            if self.config.compare_with_pg:
                self.report += \
                    f"[red]#Result hash#: `{query.result_hash}` (default) vs `{best_optimization.result_hash}` (best) vs `{query.postgres_query.result_hash}` (pg)" \
                        if query.postgres_query.result_hash != query.result_hash else \
                        f"Result hash: `{query.result_hash}` (default) vs `{best_optimization.result_hash}` (best) vs `{query.postgres_query.result_hash}` (pg)"
            elif best_optimization.result_hash != query.result_hash:
                self.report += f"[red]#Result hash#: `{query.result_hash}` (default) vs `{best_optimization.result_hash}` (best)"
            else:
                self.report += f"Result hash: `{query.result_hash}` (default) vs `{best_optimization.result_hash}` (best)"

        self._end_table_row()

        self._start_table_row()
        self.report += f"Optimizer cost: `{query.optimizer_score}` (default) vs `{best_optimization.optimizer_score}` (best)"
        self._end_table_row()

        self.report += "\n"

        self._start_table_row()
        self.report += f"Execution time: `{query.execution_time_ms}` (default) vs `{best_optimization.execution_time_ms}` (best)"
        self._end_table_row()

        self._start_table_row()

        if self.config.compare_with_pg:
            # todo do we need to report just plan?
            # self._start_collapsible("Postgres plan")
            # self._start_source(["diff"])
            # self.report += query.postgres_query.execution_plan
            # self._end_source()
            # self._end_collapsible()

            self._start_collapsible("Postgres plan diff")
            self._start_source(["diff"])
            # postgres plan should be red
            self.report += self._get_plan_diff(query.postgres_query.execution_plan.full_str,
                                               query.execution_plan.full_str, )
            self._end_source()
            self._end_collapsible()

        if show_best:
            self.__report_heatmap(query)

        self._start_collapsible("Original plan")
        self._start_source(["diff"])
        self.report += query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Best plan")
        self._start_source(["diff"])
        self.report += best_optimization.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(query.execution_plan.full_str,
                                   best_optimization.execution_plan.full_str)
        if not diff:
            diff = query.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
