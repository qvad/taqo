import numpy as np
from typing import Type

from matplotlib import pyplot as plt
from matplotlib import rcParams
from sql_formatter.core import format_sql

from db.postgres import PostgresQuery
from objects import CollectResult, Query
from reports.abstract import Report
from utils import allowed_diff


class ScoreReport(Report):
    def __init__(self):
        super().__init__()

        self.queries = {}
        self.overall_plots = {
            'color': 'k.',
            'x_values': [],
            'y_values': []
        }

    @classmethod
    def generate_report(cls, loq: CollectResult, pg_loq: CollectResult = None):
        report = ScoreReport()

        report.define_version(loq.db_version)
        report.report_model(loq.model_queries)
        report.report_config(loq.config, "YB")
        report.report_config(pg_loq.config, "PG")

        for qid, query in enumerate(loq.queries):
            report.add_query(query, pg_loq.find_query_by_hash(query.query_hash) if pg_loq else None)

        report.build_report()
        report.build_xls_report()

        report.publish_report("score")

    def get_report_name(self):
        return "score"

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    def calculate_score(self, query):
        if query.execution_time_ms == 0:
            return -1
        else:
            return "{:.2f}".format(
                query.get_best_optimization(
                    self.config).execution_time_ms / query.execution_time_ms)

    def create_default_query_plots(self):
        file_names = ['imgs/all_queries_defaults_yb.png',
                      'imgs/all_queries_defaults_pg.png']

        for i in range(2):
            x_data = []
            y_data = []

            for tag, queries in self.queries.items():
                for yb_pg_queries in queries:
                    query = yb_pg_queries[i]
                    if query and query.execution_time_ms:
                        x_data.append(query.execution_plan.get_estimated_cost())
                        y_data.append(query.execution_time_ms)

            if x_data and y_data:
                fig = self.generate_regression_and_standard_errors(x_data, y_data)
                fig.savefig(f"report/{self.start_date}/{file_names[i]}", dpi=300)
                plt.close()

        return file_names

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

    def create_query_plot(self, best_optimization, optimizations, query, scale=""):
        if not optimizations:
            return "NO PLOT"

        rcParams['font.family'] = 'serif'
        rcParams['font.size'] = 6
        plt.xlabel('Execution time [ms]')
        plt.ylabel('Predicted cost')

        if scale:
            plt.xscale(scale)
            plt.yscale(scale)

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms != 0],
                 [q.execution_plan.get_estimated_cost() for q in optimizations if
                  q.execution_time_ms != 0], 'k.',
                 [query.execution_time_ms],
                 [query.execution_plan.get_estimated_cost()], 'r^',
                 [best_optimization.execution_time_ms],
                 [best_optimization.execution_plan.get_estimated_cost()], 'go')

        file_name = f'imgs/query_{self.reported_queries_counter}{scale}.png'
        plt.savefig(f"report/{self.start_date}/{file_name}", dpi=300)
        plt.close()

        return file_name

    def add_query(self, query: Type[Query], pg: Type[Query] | None):
        if query.tag not in self.queries:
            self.queries[query.tag] = [[query, pg], ]
        else:
            self.queries[query.tag].append([query, pg])

    def build_report(self):
        self._start_table("2")
        self.report += "|Yugabyte|Postgres\n"
        default_query_plots = self.create_default_query_plots()
        self.report += f"a|image::{default_query_plots[0]}[Yugabyte,align=\"center\"]\n"
        self.report += f"a|image::{default_query_plots[1]}[Postgres,align=\"center\"]\n"
        self._end_table()

        self.report += "\n== QO score\n"

        yb_bests = 0
        pg_bests = 0
        qe_bests_geo = 1
        qo_yb_bests_geo = 1
        qo_pg_bests_geo = 1
        total = 0
        for queries in self.queries.values():
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                pg_success = pg_query.execution_time_ms != 0

                qe_bests_geo *= yb_best.execution_time_ms / pg_best.execution_time_ms if pg_success else 1
                qo_yb_bests_geo *= (yb_query.execution_time_ms if yb_query.execution_time_ms > 0 else 1.0) / \
                                   (yb_best.execution_time_ms if yb_best.execution_time_ms > 0 else 1)
                qo_pg_bests_geo *= pg_query.execution_time_ms / pg_best.execution_time_ms if pg_best.execution_time_ms != 0 else 9999999
                yb_bests += 1 if yb_query.compare_plans(yb_best.execution_plan) else 0
                pg_bests += 1 if pg_success and pg_query.compare_plans(
                    pg_best.execution_plan) else 0

                total += 1

        self._start_table("4,1,1")
        self.report += "|Statistic|YB|PG\n"
        self.report += f"|Best execution plan picked|{'{:.2f}'.format(float(yb_bests) * 100 / total)}%|{'{:.2f}'.format(float(pg_bests) * 100 / total)}%\n"
        self.report += f"|Geomeric mean QE best\n2+m|{'{:.2f}'.format(qe_bests_geo ** (1 / total))}\n"
        self.report += f"|Geomeric mean QO default vs best|{'{:.2f}'.format(qo_yb_bests_geo ** (1 / total))}|{'{:.2f}'.format(qo_pg_bests_geo ** (1 / total))}\n"
        self._end_table()

        self.report += "\n[#top]\n== QE score\n"

        num_columns = 7
        for tag, queries in self.queries.items():
            self._start_table("1,1,1,1,1,1,4")
            self.report += "|YB|YB Best|PG|PG Best|Ratio YB vs PG|Ratio Best YB vs PG|Query\n"
            self.report += f"{num_columns}+m|{tag}.sql\n"
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                pg_success = pg_query.execution_time_ms != 0

                default_yb_equality = "[green]" if yb_query.compare_plans(
                    yb_best.execution_plan) else "[red]"
                default_pg_equality = "[green]" if pg_success and pg_query.compare_plans(
                    pg_best.execution_plan) else "[red]"

                best_yb_pg_equality = "(eq) " if yb_best.compare_plans(
                    pg_best.execution_plan) else ""

                ratio_x3 = yb_query.execution_time_ms / (
                        3 * pg_query.execution_time_ms) if pg_query.execution_time_ms != 0 else 99999999
                ratio_x3_str = "{:.2f}".format(
                    yb_query.execution_time_ms / pg_query.execution_time_ms if pg_query.execution_time_ms != 0 else 99999999)
                ratio_color = "[green]" if ratio_x3 <= 1.0 else "[red]"

                ratio_best = yb_best.execution_time_ms / (
                        3 * pg_best.execution_time_ms) \
                    if yb_best.execution_time_ms != 0 and pg_success else 99999999
                ratio_best_x3_str = "{:.2f}".format(
                    yb_best.execution_time_ms / pg_best.execution_time_ms
                    if yb_best.execution_time_ms != 0 and pg_success else 99999999)
                ratio_best_color = "[green]" if ratio_best <= 1.0 else "[red]"

                bitmap_flag = "[blue]" if pg_success and "bitmap" in pg_query.execution_plan.full_str.lower() else "[black]"

                self.report += f"a|[black]#*{'{:.2f}'.format(yb_query.execution_time_ms)}*#\n" \
                               f"a|{default_yb_equality}#*{'{:.2f}'.format(yb_best.execution_time_ms)}*#\n" \
                               f"a|{bitmap_flag}#*{'{:.2f}'.format(pg_query.execution_time_ms)}*#\n" \
                               f"a|{default_pg_equality}#*{'{:.2f}'.format(pg_best.execution_time_ms)}*#\n" \
                               f"a|{ratio_color}#*{ratio_x3_str}*#\n" \
                               f"a|{ratio_best_color}#*{best_yb_pg_equality}{ratio_best_x3_str}*#\n"
                self.report += f"a|[#{yb_query.query_hash}_top]\n<<{yb_query.query_hash}>>\n"
                self._start_source(["sql"])
                self.report += format_sql(pg_query.query.replace("|", "\|"))
                self._end_source()
                self.report += "\n"
                self._end_table_row()

            self._end_table()

        # different results links
        for tag in self.queries.keys():
            self.report += f"\n<<{tag}>>\n"

        for tag, queries in self.queries.items():
            self.report += f"\n[#{tag}]\n== {tag} queries file\n\n"
            for query in queries:
                self.__report_query(query[0], query[1], True)

    def __report_near_queries(self, query: Type[Query]):
        if query.optimizations:
            best_optimization = query.get_best_optimization(self.config)
            if add_to_report := "".join(
                    f"`{optimization.explain_hints}`\n\n"
                    for optimization in query.optimizations
                    if allowed_diff(self.config, best_optimization.execution_time_ms,
                                    optimization.execution_time_ms)):
                self._start_collapsible("Near best optimization hints")
                self.report += add_to_report
                self._end_collapsible()
    def build_xls_report(self):
        import xlsxwriter

        workbook = xlsxwriter.Workbook(f'report/{self.start_date}/report_score.xls')
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

        eq_good_format = workbook.add_format()
        eq_good_format.set_bold()
        eq_good_format.set_bg_color('#d9ead3')

        bm_format = workbook.add_format()
        bm_format.set_bold()
        bm_format.set_bg_color('#cfe2f3')

        pg_comparison_format = workbook.add_format()
        pg_comparison_format.set_bold()
        pg_comparison_format.set_bg_color('#fce5cd')

        # Start from the first cell. Rows and columns are zero indexed.
        yb_bests = 0
        pg_bests = 0
        total = 0
        for queries in self.queries.values():
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config, )
                pg_best = pg_query.get_best_optimization(self.config, )

                yb_bests += 1 if yb_query.compare_plans(yb_best.execution_plan) else 0
                pg_bests += 1 if pg_query.compare_plans(pg_best.execution_plan) else 0

                total += 1

        worksheet.write(0, 0, "YB", head_format)
        worksheet.write(0, 1, "YB Best", head_format)
        worksheet.write(0, 2, "PG", head_format)
        worksheet.write(0, 3, "PG Best", head_format)
        worksheet.write(0, 4, "Ratio YB vs PG", head_format)
        worksheet.write(0, 5, "Best YB vs PG", head_format)
        worksheet.write(0, 6, "Query", head_format)
        worksheet.write(0, 7, "Query Hash", head_format)

        row = 1
        # Iterate over the data and write it out row by row.
        for tag, queries in self.queries.items():
            for query in queries:
                yb_query: PostgresQuery = query[0]
                pg_query: PostgresQuery = query[1]

                yb_best = yb_query.get_best_optimization(self.config, )
                pg_best = pg_query.get_best_optimization(self.config, )

                default_yb_equality = yb_query.compare_plans(yb_best.execution_plan)
                default_pg_equality = pg_query.compare_plans(pg_best.execution_plan)

                default_yb_pg_equality = yb_query.compare_plans(pg_query.execution_plan)
                best_yb_pg_equality = yb_best.compare_plans(pg_best.execution_plan)

                ratio_x3 = yb_query.execution_time_ms / (
                        3 * pg_query.execution_time_ms) if pg_query.execution_time_ms != 0 else 99999999
                ratio_x3_str = "{:.2f}".format(
                    yb_query.execution_time_ms / pg_query.execution_time_ms if pg_query.execution_time_ms != 0 else 99999999)
                ratio_color = ratio_x3 > 1.0

                ratio_best = yb_best.execution_time_ms / (
                        3 * pg_best.execution_time_ms) if yb_best.execution_time_ms != 0 else 99999999
                ratio_best_x3_str = "{:.2f}".format(
                    yb_best.execution_time_ms / pg_best.execution_time_ms if yb_best.execution_time_ms != 0 else 99999999)
                ratio_best_color = ratio_best > 1.0

                bitmap_flag = "bitmap" in pg_query.execution_plan.full_str.lower()

                best_pg_format = None
                if ratio_best_color and best_yb_pg_equality:
                    best_pg_format = eq_bad_format
                elif best_yb_pg_equality:
                    best_pg_format = eq_good_format
                elif ratio_best_color:
                    best_pg_format = pg_comparison_format

                df_pf_format = None
                if ratio_color and default_yb_pg_equality:
                    df_pf_format = eq_bad_format
                elif default_yb_pg_equality:
                    df_pf_format = eq_good_format
                elif ratio_color:
                    df_pf_format = pg_comparison_format

                worksheet.write(row, 0, '{:.2f}'.format(yb_query.execution_time_ms))
                worksheet.write(row, 1,
                                f"{'{:.2f}'.format(yb_best.execution_time_ms)}",
                                eq_format if default_yb_equality else None)
                worksheet.write(row, 2,
                                f"{'{:.2f}'.format(pg_query.execution_time_ms)}",
                                bm_format if bitmap_flag else None)
                worksheet.write(row, 3,
                                f"{'{:.2f}'.format(pg_best.execution_time_ms)}",
                                eq_format if default_pg_equality else None)
                worksheet.write(row, 4, f"{ratio_x3_str}", df_pf_format)
                worksheet.write(row, 5, f"{ratio_best_x3_str}", best_pg_format)
                worksheet.write(row, 6, f'{format_sql(pg_query.query)}')
                worksheet.write(row, 7, f'{pg_query.query_hash}')
                row += 1

        workbook.close()

    def __report_heatmap(self, query: Type[Query]):
        """
        Here is the deal. In PG plans we can separate each plan tree node by splitting by `->`
        When constructing heatmap need to add + or - to the beginning of string `\n`.
        So there is 2 splitters - \n and -> and need to construct correct result.

        :param query:
        :return:
        """
        # TODO FIX THIS!!!!!
        if not (execution_plan_heatmap := query.heatmap()):
            return

        best_decision = max(row['weight'] for row in execution_plan_heatmap.values())
        last_rowid = max(execution_plan_heatmap.keys())
        result = ""
        for row_id, row in execution_plan_heatmap.items():
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
    def __report_query(self, yb_query: Type[Query], pg_query: Type[Query], show_best: bool):
        yb_best = yb_query.get_best_optimization(self.config)

        self.reported_queries_counter += 1

        self.report += f"\n[#{yb_query.query_hash}]\n"
        self.report += f"=== Query {yb_query.query_hash}"
        self.report += f"\n{yb_query.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self.report += f"\n<<{yb_query.query_hash}_top,Show in summary>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(yb_query.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()
        self.report += f"YB Default explain hints - `{yb_query.explain_hints}`"
        self._add_double_newline()

        if show_best:
            self._add_double_newline()
            self.report += f"YB Best explain hints - `{yb_best.explain_hints}`"
            self._add_double_newline()

            self.__report_near_queries(yb_query)

        self._start_table("2")
        self.report += "|Default|Log scale\n"
        query_plot = self.create_query_plot(yb_best, yb_query.optimizations, yb_query)
        query_plot_log = self.create_query_plot(yb_best, yb_query.optimizations, yb_query, "log")
        self.report += f"a|image::{query_plot}[Default,align=\"center\"]\n"
        self.report += f"a|image::{query_plot_log}[Log scale,align=\"center\"]\n"
        self._end_table()

        self._add_double_newline()

        self._add_double_newline()
        default_yb_equality = "(eq) " if yb_query.compare_plans(
            yb_best.execution_plan) else ""
        default_pg_equality = ""
        default_yb_pg_equality = ""

        best_yb_pg_equality = ""
        if pg_query and pg_query.execution_time_ms != 0:
            self._start_table("5")
            self.report += "|Metric|YB|YB Best|PG|PG Best\n"

            pg_best = pg_query.get_best_optimization(self.config)
            default_pg_equality = "(eq) " if pg_query.compare_plans(
                pg_best.execution_plan) else ""
            best_yb_pg_equality = "(eq) " if yb_best.compare_plans(
                pg_best.execution_plan) else ""
            default_yb_pg_equality = "(eq) " if yb_query.compare_plans(
                pg_query.execution_plan) else ""

            if 'order by' in yb_query.query:
                self._start_table_row()
                self.report += \
                    f"!! Result hash|{yb_query.result_hash}|{yb_best.result_hash}|{pg_query.result_hash}|{pg_best.result_hash}" \
                        if pg_query.result_hash != yb_query.result_hash else \
                        f"Result hash|`{yb_query.result_hash}|{yb_best.result_hash}|{pg_query.result_hash}|{pg_best.result_hash}"
                self._end_table_row()

            self._start_table_row()
            self.report += f"Cardinality|{yb_query.result_cardinality}|{yb_best.result_cardinality}|{pg_query.result_cardinality}|{pg_best.result_cardinality}"
            self._end_table_row()
            self._start_table_row()
            self.report += f"Estimated cost|{yb_query.execution_plan.get_estimated_cost()}|{default_yb_equality}{yb_best.execution_plan.get_estimated_cost()}|{pg_query.execution_plan.get_estimated_cost()}|{default_pg_equality}{pg_best.execution_plan.get_estimated_cost()}"
            self._end_table_row()
            self._start_table_row()
            self.report += f"Execution time|{'{:.2f}'.format(yb_query.execution_time_ms)}|{default_yb_equality}{'{:.2f}'.format(yb_best.execution_time_ms)}|{'{:.2f}'.format(pg_query.execution_time_ms)}|{default_pg_equality}{'{:.2f}'.format(pg_best.execution_time_ms)}"
            self._end_table_row()
        else:
            self._start_table("3")
            self.report += "|Metric|YB|YB Best\n"

            if yb_best.result_hash != yb_query.result_hash:
                self.report += f"!! Result hash|{yb_query.result_hash}|{yb_best.result_hash}"
            else:
                self.report += f"Result hash|{yb_query.result_hash}|{yb_best.result_hash}"
            self._end_table_row()

            self._start_table_row()
            self.report += f"Cardinality|{yb_query.result_cardinality}|{yb_best.result_cardinality}"
            self._end_table_row()
            self._start_table_row()
            self.report += f"Optimizer cost|{yb_query.execution_plan.get_estimated_cost()}|{default_yb_equality}{yb_best.execution_plan.get_estimated_cost()}"
            self._end_table_row()
            self._start_table_row()
            self.report += f"Execution time|{yb_query.execution_time_ms}|{default_yb_equality}{yb_best.execution_time_ms}"
            self._end_table_row()
        self._end_table()

        self._start_table()
        self._start_table_row()

        if pg_query and pg_query.execution_time_ms != 0:
            bitmap_used = "(bm) " if "bitmap" in pg_query.execution_plan.full_str.lower() else ""
            self._start_collapsible(f"{bitmap_used}PG plan")
            self._start_source(["diff"])
            self.report += pg_query.execution_plan.full_str
            self._end_source()
            self._end_collapsible()

            pg_best = pg_query.get_best_optimization(self.config)
            bitmap_used = "(bm) " if "bitmap" in pg_best.execution_plan.full_str.lower() else ""
            self._start_collapsible(f"{default_pg_equality}{bitmap_used}PG best")
            self._start_source(["diff"])
            self.report += pg_best.execution_plan.full_str
            self._end_source()
            self._end_collapsible()

            self._start_collapsible(f"{default_yb_pg_equality}PG default vs YB default")
            self._start_source(["diff"])
            # postgres plan should be red
            self.report += self._get_plan_diff(
                pg_query.execution_plan.full_str,
                yb_query.execution_plan.full_str,
            )
            self._end_source()
            self._end_collapsible()

            self._start_collapsible(f"{best_yb_pg_equality}PG best vs YB best")
            self._start_source(["diff"])
            self.report += self._get_plan_diff(
                pg_best.execution_plan.full_str,
                yb_best.execution_plan.full_str,
            )
            self._end_source()
            self._end_collapsible()

        if show_best:
            self.__report_heatmap(yb_query)

        self._start_collapsible("YB default plan")
        self._start_source(["diff"])
        self.report += yb_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible(f"YB best plan")
        self._start_source(["diff"])
        self.report += yb_best.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self.report += f"{default_yb_equality}YB default vs YB best\n"
        self._start_source(["diff"])
        diff = self._get_plan_diff(
            yb_query.execution_plan.full_str,
            yb_best.execution_plan.full_str
        )
        if not diff:
            diff = yb_query.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
