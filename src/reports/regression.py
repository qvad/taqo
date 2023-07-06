from dataclasses import dataclass
from typing import Type

import numpy as np
from sql_formatter.core import format_sql
from matplotlib import pyplot as plt
from matplotlib import rcParams

from collect import CollectResult
from objects import Query
from reports.abstract import Report


@dataclass
class ShortSummaryReport:
    diff_plans: int = 0
    diff_rpc_calls: int = 0
    diff_wait_times: int = 0
    diff_scanned_rows: int = 0
    diff_peak_memory: int = 0


class RegressionReport(Report):
    def __init__(self):
        super().__init__()

        self.v1_name = None
        self.v2_name = None
        self.queries = {}
        self.short_summary = ShortSummaryReport()

    @classmethod
    def generate_report(cls,
                        v1_name: str,
                        v2_name: str,
                        loq_v1: CollectResult,
                        loq_v2: CollectResult):
        report = RegressionReport()

        report.define_version_names(v1_name, v2_name)
        report.define_version(loq_v1.db_version, loq_v2.db_version)

        report.report_config(loq_v1.config, v1_name)
        report.report_config(loq_v2.config, v2_name)

        report.report_model(loq_v1.model_queries)

        for query in loq_v1.queries:
            report.add_query(query, loq_v2.find_query_by_hash(query.query_hash) if loq_v2 else None)

        report.build_report()
        report.build_xls_report()

        report.publish_report("regression")
        report.publish_short_report()

    def get_report_name(self):
        return "Regression"

    def define_version(self, first_version, second_version):
        self.report += f"[GIT COMMIT/VERSION]\n====\n" \
                       f"First:\n{first_version}\n\nSecond:\n{second_version}\n====\n\n"

    def add_query(self, first_query: Type[Query], second_query: Type[Query]):
        if first_query.tag not in self.queries:
            self.queries[first_query.tag] = [[first_query, second_query], ]
        else:
            self.queries[first_query.tag].append([first_query, second_query])

    def create_query_plot(self, best_optimization, optimizations, query, postfix_name):
        if not optimizations:
            return "NO PLOT"

        rcParams['font.family'] = 'serif'
        rcParams['font.size'] = 6
        plt.xlabel('Execution time [ms]')
        plt.ylabel('Predicted cost')

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms != 0],
                 [q.execution_plan.get_estimated_cost() for q in optimizations if
                  q.execution_time_ms != 0], 'k.',
                 [query.execution_time_ms],
                 [query.execution_plan.get_estimated_cost()], 'r^',
                 [best_optimization.execution_time_ms],
                 [best_optimization.execution_plan.get_estimated_cost()], 'go')

        file_name = f'imgs/query_{self.reported_queries_counter}_{postfix_name}.png'
        plt.savefig(f"report/{self.start_date}/{file_name}", dpi=300)
        plt.close()

        return file_name

    @staticmethod
    def fix_last_newline_in_result(result, rows):
        if result:
            splitted_result = result.split("\n")
            result = "\n".join(splitted_result[:-1])
            last_newline = splitted_result[-1]
            rows[0] = f"{last_newline}{rows[0]}"
            result += "\n"

        return result

    @staticmethod
    def generate_regression_and_standard_errors(x_data, y_data):
        x = np.array(x_data)
        y = np.array(y_data)
        n = x.size

        a, b = np.polyfit(x, y, deg=1)
        y_est = a * x + b
        y_err = (y - y_est).std() * np.sqrt(1 / n + (x - x.mean()) ** 2 / np.sum((x - x.mean()) ** 2))

        fig, ax = plt.subplots()

        plt.xlabel('Predicted cost')
        plt.ylabel('Execution time [ms]')

        ax.plot(x, y_est, '-')
        ax.fill_between(x, y_est - y_err, y_est + y_err, alpha=0.2)
        ax.plot(x, y, 'k.')

        return fig

    def create_default_query_plots(self):
        file_names = ['imgs/all_queries_defaults_yb_v1.png',
                      'imgs/all_queries_defaults_yb_v2.png']

        for i in range(2):
            x_data = []
            y_data = []

            for tag, queries in self.queries.items():
                for yb_pg_queries in queries:
                    query = yb_pg_queries[i]
                    if query.execution_time_ms:
                        x_data.append(query.execution_plan.get_estimated_cost())
                        y_data.append(query.execution_time_ms)

            fig = self.generate_regression_and_standard_errors(x_data, y_data)
            fig.savefig(f"report/{self.start_date}/{file_names[i]}", dpi=300)
            plt.close()

        return file_names

    def build_report(self):
        # link to top
        self.add_plan_comparison()
        self.add_rpc_calls()
        self.add_rpc_wait_times()
        self.add_scanned_rows()
        self.add_peak_memory_collapsible()

        self._start_table("2")
        self.report += f"|{self.v1_name}|{self.v2_name}\n"
        default_query_plots = self.create_default_query_plots()
        self.report += f"a|image::{default_query_plots[0]}[{self.v1_name},align=\"center\"]\n"
        self.report += f"a|image::{default_query_plots[1]}[{self.v2_name},align=\"center\"]\n"
        self._end_table()

        self.report += "\n== QO score\n"

        yb_v1_bests = 0
        yb_v2_bests = 0
        qe_bests_geo = 1
        qe_default_geo = 1
        qo_yb_v1_bests = 1
        qo_yb_v2_bests = 1
        total = 0

        v2_has_optimizations = True

        for queries in self.queries.values():
            for query in queries:
                yb_v1_query = query[0]
                yb_v2_query = query[1]

                if not yb_v2_query.optimizations:
                    v2_has_optimizations = False

                yb_v1_best = yb_v1_query.get_best_optimization(self.config)
                yb_v2_best = yb_v2_query.get_best_optimization(self.config) if v2_has_optimizations else yb_v1_best

                qe_default_geo *= yb_v2_query.execution_time_ms / yb_v1_query.execution_time_ms
                qe_bests_geo *= yb_v2_best.execution_time_ms / yb_v1_best.execution_time_ms
                qo_yb_v1_bests *= (yb_v1_query.execution_time_ms
                                   if yb_v1_query.execution_time_ms > 0 else 1.0) / \
                                  (yb_v1_best.execution_time_ms
                                   if yb_v1_best.execution_time_ms > 0 else 1)
                qo_yb_v2_bests *= yb_v2_query.execution_time_ms / yb_v2_best.execution_time_ms \
                    if yb_v2_best.execution_time_ms != 0 else 9999999
                yb_v1_bests += 1 if yb_v1_query.compare_plans(yb_v1_best.execution_plan) else 0
                yb_v2_bests += 1 if yb_v2_query.compare_plans(yb_v2_best.execution_plan) else 0

                total += 1

        self._start_table("4,1,1")
        self.report += f"|Statistic|{self.v1_name}|{self.v2_name}\n"
        self.report += f"|Best execution plan picked|{'{:.2f}'.format(float(yb_v1_bests) * 100 / total)}%" \
                       f"|{'{:.2f}'.format(float(yb_v2_bests) * 100 / total)}%\n"
        self.report += f"|Geomeric mean QE default\n" \
                       f"2+m|{'{:.2f}'.format(qe_default_geo ** (1 / total))}\n"

        if v2_has_optimizations:
            self.report += f"|Geomeric mean QE best\n" \
                           f"2+m|{'{:.2f}'.format(qe_bests_geo ** (1 / total))}\n"

        self.report += f"|Geomeric mean QO default vs best" \
                       f"|{'{:.2f}'.format(qo_yb_v1_bests ** (1 / total))}" \
                       f"|{'{:.2f}'.format(qo_yb_v2_bests ** (1 / total))}\n"
        self._end_table()

        self.report += "\n[#top]\n== QE score\n"

        num_columns = 7 if v2_has_optimizations else 6
        v2_prefix = "Best" if v2_has_optimizations else "Default"
        v2_best_col = f"|{self.v2_name} {v2_prefix}" if v2_has_optimizations else ""
        table_layout = "1,1,1,1,1,1,4" if v2_has_optimizations else "1,1,1,1,1,4"
        for tag, queries in self.queries.items():
            self._start_table(table_layout)
            self.report += f"|{self.v1_name}" \
                           f"|{self.v1_name} Best" \
                           f"|{self.v2_name}" \
                           f"{v2_best_col}" \
                           f"|Ratio {self.v2_name} vs Default {self.v1_name}" \
                           f"|Ratio {v2_prefix} {self.v2_name} vs Best {self.v1_name}" \
                           f"|Query\n"
            self.report += f"{num_columns}+m|{tag}.sql\n"
            for query in queries:
                yb_v1_query = query[0]
                yb_v2_query = query[1]

                yb_v1_best = yb_v1_query.get_best_optimization(self.config)
                yb_v2_best = yb_v2_query.get_best_optimization(self.config) if v2_has_optimizations else yb_v1_best

                success = yb_v2_query.execution_time_ms != 0

                default_v1_equality = "[green]" \
                    if yb_v1_query.compare_plans(yb_v1_best.execution_plan) else "[red]"
                default_v2_equality = "[green]" \
                    if success and yb_v2_query.compare_plans(yb_v2_best.execution_plan) else "[red]"

                if v2_has_optimizations:
                    best_yb_pg_equality = "(eq) " if yb_v1_best.compare_plans(yb_v2_best.execution_plan) else ""
                else:
                    best_yb_pg_equality = "(eq) " if yb_v1_best.compare_plans(yb_v2_query.execution_plan) else ""

                ratio_x3 = yb_v2_query.execution_time_ms / yb_v1_query.execution_time_ms \
                    if yb_v1_query.execution_time_ms != 0 else 99999999
                ratio_x3_str = "{:.2f}".format(yb_v2_query.execution_time_ms / yb_v1_query.execution_time_ms
                                               if yb_v2_query.execution_time_ms != 0 else 99999999)
                ratio_color = "[green]" if ratio_x3 <= 1.0 else "[red]"

                if v2_has_optimizations:
                    ratio_best = yb_v2_best.execution_time_ms / yb_v1_best.execution_time_ms \
                        if yb_v1_best.execution_time_ms != 0 and success else 99999999
                    ratio_best_x3_str = "{:.2f}".format(yb_v2_best.execution_time_ms / yb_v1_best.execution_time_ms
                                                        if yb_v1_best.execution_time_ms != 0 and success else 99999999)
                    ratio_best_color = "[green]" if ratio_best <= 1.0 else "[red]"
                else:
                    ratio_best = yb_v2_query.execution_time_ms / yb_v1_best.execution_time_ms \
                        if yb_v1_best.execution_time_ms != 0 and success else 99999999
                    ratio_best_x3_str = "{:.2f}".format(yb_v2_query.execution_time_ms / yb_v1_best.execution_time_ms
                                                        if yb_v1_best.execution_time_ms != 0 and success else 99999999)
                    ratio_best_color = "[green]" if ratio_best <= 1.0 else "[red]"

                bitmap_flag = "[blue]" \
                    if success and "bitmap" in yb_v2_query.execution_plan.full_str.lower() else "[black]"

                b2_best_col = f"a|{default_v2_equality}#*{'{:.2f}'.format(yb_v2_best.execution_time_ms)}*#\n" if v2_has_optimizations else ""
                self.report += f"a|[black]#*{'{:.2f}'.format(yb_v1_query.execution_time_ms)}*#\n" \
                               f"a|{default_v1_equality}#*{'{:.2f}'.format(yb_v1_best.execution_time_ms)}*#\n" \
                               f"a|{bitmap_flag}#*{'{:.2f}'.format(yb_v2_query.execution_time_ms)}*#\n" \
                               f"{b2_best_col}" \
                               f"a|{ratio_color}#*{ratio_x3_str}*#\n" \
                               f"a|{ratio_best_color}#*{best_yb_pg_equality}{ratio_best_x3_str}*#\n"
                self.report += f"a|[#{yb_v1_query.query_hash}_top]\n<<{yb_v1_query.query_hash}>>\n"
                self._start_source(["sql"])
                self.report += format_sql(yb_v2_query.query.replace("|", "\|"))
                self._end_source()
                self.report += "\n"
                self._end_table_row()

            self._end_table()

        for tag, queries in self.queries.items():
            self.report += f"\n== {tag} queries file\n\n"
            for query in queries:
                self.__report_query(query[0], query[1])

    def add_plan_comparison(self):
        self._start_collapsible("Plan comparison")
        self.report += "\n[#plans_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(1 for query in queries
                                 if query[0].compare_plans(query[1].execution_plan))
            self.report += f"a|<<{tag}>>\n"
            self.short_summary.diff_plans = len(queries) - num_same_plans
            color = "[green]" if self.short_summary.diff_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{self.short_summary.diff_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_rpc_calls(self):
        self._start_collapsible("RPC Calls")
        self.report += "\n[#rpc_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            self.short_summary.diff_rpc_calls = sum(
                query[0].execution_plan.get_rpc_calls() != query[1].execution_plan.get_rpc_calls()
                for query in queries
            )
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if self.short_summary.diff_rpc_calls == 0 else "[orange]"
            self.report += f"a|{color}#*{self.short_summary.diff_rpc_calls}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_rpc_wait_times(self):
        self._start_collapsible("RPC Wait Times")
        self.report += "\n[#rpc_wait_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            self.short_summary.diff_wait_times = sum(
                query[0].execution_plan.get_rpc_wait_times() != query[1].execution_plan.get_rpc_wait_times()
                for query in queries
            )
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if self.short_summary.diff_wait_times == 0 else "[orange]"
            self.report += f"a|{color}#*{self.short_summary.diff_wait_times}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_scanned_rows(self):
        self._start_collapsible("Scanned rows")
        self.report += "\n[#rows_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(
                query[0].execution_plan.get_scanned_rows() != query[1].execution_plan.get_scanned_rows()
                for query in queries
            )
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if num_same_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{num_same_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_peak_memory_collapsible(self):
        self._start_collapsible("Peak memory")
        self.report += "\n[#memory_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            self.short_summary.diff_peak_memory = sum(
                query[0].execution_plan.get_peak_memory() != query[1].execution_plan.get_peak_memory()
                for query in queries
            )
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if self.short_summary.diff_peak_memory == 0 else "[orange]"
            self.report += f"a|{color}#*{self.short_summary.diff_peak_memory}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    # noinspection InsecureHash
    def __report_query(self, v1_query: Type[Query], v2_query: Type[Query]):
        v2_has_optimizations = v2_query.optimizations is not None

        v1_best = v1_query.get_best_optimization(self.config)
        v2_best = v2_query.get_best_optimization(self.config) if v2_has_optimizations else v1_best

        self.reported_queries_counter += 1

        self.report += f"\n[#{v1_query.query_hash}]\n"
        self.report += f"=== Query {v1_query.query_hash}"
        self.report += f"\n{v1_query.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self.report += f"\n<<{v1_query.query_hash}_top,Show in summary>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(v1_query.query.replace("|", "\|"))
        self._end_source()

        if v2_has_optimizations:
            self._start_table("2")
            self.report += f"|{self.v1_name}|{self.v2_name}\n"
            v1_query_plot = self.create_query_plot(v1_best, v1_query.optimizations, v1_query, "v1")
            v2_query_plot = self.create_query_plot(v2_best, v2_query.optimizations, v2_query, "v2")
            self.report += f"a|image::{v1_query_plot}[{self.v1_name},align=\"center\"]\n"
            self.report += f"a|image::{v2_query_plot}[{self.v2_name},align=\"center\"]\n"
            self._end_table()
        else:
            self._start_table("1")
            self.report += f"|{self.v1_name}\n"
            v1_query_plot = self.create_query_plot(v1_best, v1_query.optimizations, v1_query, "v1")
            self.report += f"a|image::{v1_query_plot}[{self.v1_name},align=\"center\",width=640,height=480]\n"
            self._end_table()

        self._add_double_newline()

        self._add_double_newline()
        default_v1_equality = "(eq) " if v1_query.compare_plans(v1_best.execution_plan) else ""

        self._start_table("5")
        self.report += f"|Metric|{self.v1_name}|{self.v1_name} Best|{self.v2_name}|{self.v2_name} Best\n"

        default_v2_equality = "(eq) " if v2_query.compare_plans(v2_best.execution_plan) else ""
        best_yb_pg_equality = "(eq) " if v1_best.compare_plans(v2_best.execution_plan) else ""
        default_v1_v2_equality = "(eq) " if v1_query.compare_plans(v2_query.execution_plan) else ""

        if 'order by' in v1_query.query:
            self._start_table_row()
            self.report += f"!! Result hash" \
                           f"|{v1_query.result_hash}" \
                           f"|{v1_best.result_hash}" \
                           f"|{v2_query.result_hash}" \
                           f"|{v2_best.result_hash}" \
                if v2_query.result_hash != v1_query.result_hash else \
                f"Result hash" \
                f"|`{v1_query.result_hash}" \
                f"|{v1_best.result_hash}" \
                f"|{v2_query.result_hash}" \
                f"|{v2_best.result_hash}"
            self._end_table_row()

        self._start_table_row()
        self.report += f"Cardinality" \
                       f"|{v1_query.result_cardinality}" \
                       f"|{v1_best.result_cardinality}" \
                       f"|{v2_query.result_cardinality}" \
                       f"|{v2_best.result_cardinality}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Estimated cost" \
                       f"|{v1_query.execution_plan.get_estimated_cost()}" \
                       f"|{default_v1_equality}{v1_best.execution_plan.get_estimated_cost()}" \
                       f"|{v2_query.execution_plan.get_estimated_cost()}" \
                       f"|{default_v2_equality}{v2_best.execution_plan.get_estimated_cost()}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Execution time" \
                       f"|{'{:.2f}'.format(v1_query.execution_time_ms)}" \
                       f"|{default_v1_equality}{'{:.2f}'.format(v1_best.execution_time_ms)}" \
                       f"|{'{:.2f}'.format(v2_query.execution_time_ms)}" \
                       f"|{default_v2_equality}{'{:.2f}'.format(v2_best.execution_time_ms)}"
        self._end_table_row()

        self._end_table()

        self._start_table()
        self._start_table_row()

        self._start_collapsible(f"{self.v1_name} default plan")
        self._start_source(["diff"])
        self.report += v1_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible(f"{self.v1_name} best plan")
        self._start_source(["diff"])
        self.report += v1_best.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible(f"{self.v2_name} default plan")
        self._start_source(["diff"])
        self.report += v2_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        if v2_has_optimizations:
            v2_best = v2_query.get_best_optimization(self.config)
            self._start_collapsible(f"{default_v2_equality}{self.v2_name} best plan")
            self._start_source(["diff"])
            self.report += v2_best.execution_plan.full_str
            self._end_source()
            self._end_collapsible()

        v2_prefix = "best" if v2_has_optimizations else "default"
        self._start_collapsible(f"{best_yb_pg_equality}{self.v1_name} best vs {self.v2_name} {v2_prefix}")
        self._start_source(["diff"])
        self.report += self._get_plan_diff(
            v1_best.execution_plan.full_str,
            v2_best.execution_plan.full_str if v2_has_optimizations else v2_query.execution_plan.full_str,
        )
        self._end_source()
        self._end_collapsible()

        self.report += f"{default_v1_equality}{self.v1_name} vs {self.v2_name}\n"
        self._start_source(["diff"])
        diff = self._get_plan_diff(
            v1_query.execution_plan.full_str,
            v2_query.execution_plan.full_str
        )
        if not diff:
            diff = v1_query.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()

    def build_xls_report(self):
        import xlsxwriter

        workbook = xlsxwriter.Workbook(f'report/{self.start_date}/report_regression.xls')
        worksheet = workbook.add_worksheet()

        head_format = workbook.add_format()
        head_format.set_bold()
        head_format.set_bg_color('#999999')

        eq_format = workbook.add_format()
        eq_format.set_bold()
        eq_format.set_bg_color('#d9ead3')

        eq_bad_format = workbook.add_format()
        eq_bad_format.set_bold()
        eq_bad_format.set_bg_color('#fff2cc')

        worksheet.write(0, 0, "First", head_format)
        worksheet.write(0, 1, "Second", head_format)
        worksheet.write(0, 2, "Ratio", head_format)
        worksheet.write(0, 3, "Query", head_format)
        worksheet.write(0, 4, "Query Hash", head_format)

        row = 1
        # Iterate over the data and write it out row by row.
        for tag, queries in self.queries.items():
            for query in queries:
                first_query: Query = query[0]
                second_query: Query = query[1]

                ratio = first_query.execution_time_ms / (second_query.execution_time_ms) \
                    if first_query.execution_time_ms != 0 else 99999999
                ratio_color = eq_bad_format if ratio > 1.0 else eq_format

                worksheet.write(row, 0, '{:.2f}'.format(first_query.execution_time_ms))
                worksheet.write(row, 1, f"{'{:.2f}'.format(second_query.execution_time_ms)}")
                worksheet.write(row, 2, f'{ratio}', ratio_color)
                worksheet.write(row, 3, f'{format_sql(first_query.query)}')
                worksheet.write(row, 4, f'{first_query.query_hash}')
                row += 1

        workbook.close()

    def define_version_names(self, v1_name, v2_name):
        self.v1_name = v1_name
        self.v2_name = v2_name

    def publish_short_report(self):
        with open(f"report/{self.start_date}/short_regression_summary.txt", "w") as short_summary:
            short_summary.write(f"Changed plans: {self.short_summary.diff_plans}\n")
            short_summary.write(f"Changed scanned rows: {self.short_summary.diff_scanned_rows}\n")
            short_summary.write(f"Changed RPC calls: {self.short_summary.diff_rpc_calls}\n")
            short_summary.write(f"Changed RPC wait times: {self.short_summary.diff_wait_times}\n")
            short_summary.write(f"Changed peak memory: {self.short_summary.diff_peak_memory}\n")
