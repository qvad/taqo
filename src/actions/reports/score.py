from typing import Type

import numpy as np
from bokeh.embed import components
from bokeh.layouts import gridplot
from bokeh.models import ColumnDataSource, HoverTool, TapTool, BoxZoomTool, WheelZoomTool, PanTool, SaveTool, ResetTool
from bokeh.models import OpenURL, CDSView, GroupFilter
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from matplotlib import pyplot as plt
from matplotlib import rcParams
from scipy.stats import linregress
from sql_formatter.core import format_sql

from actions.report import AbstractReportAction
from collect import CollectResult
from db.postgres import PostgresQuery
from objects import Query
from utils import allowed_diff, get_plan_diff, extract_execution_time_from_analyze


class ScoreReport(AbstractReportAction):
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
        report.report_config(loq.database_config, "Flags")
        report.report_config(loq.config, "YB")
        report.report_config(pg_loq.config, "PG")

        report.report_model(loq.model_queries)

        for query in loq.queries:
            pg_query = pg_loq.find_query_by_hash(query.query_hash) if pg_loq else None
            if pg_query:
                report.add_query(query, pg_query)
            else:
                report.logger.exception("No PG query found for hash %s", query.query_hash)
                report.add_query(query, query.create_copy())

        report.build_report()
        report.build_xls_report()

        report.publish_report("score")

    def get_report_name(self):
        return "score"

    def define_version(self, version):
        self.content += f"[VERSION]\n====\n{version}\n====\n\n"

    def calculate_score(self, query):
        if query.execution_time_ms == 0:
            return -1
        else:
            return "{:.2f}".format(
                query.get_best_optimization(self.config).execution_time_ms / query.execution_time_ms)

    def create_default_query_plots_interactive(self):
        data = {
            'query_hash': [],
            'query_tag': [],
            'query': [],
            'yb_cost': [],
            'yb_time': [],
            'pg_cost': [],
            'pg_time': [],
        }

        data_yb_bad_plans = {
            'yb_cost': [],
            'yb_time': [],
            'query_tag': [],
        }

        data_pg_bad_plans = {
            'pg_cost': [],
            'pg_time': [],
            'query_tag': [],
        }

        tags = []

        for tag, queries in self.queries.items():
            for yb_pg_queries in queries:
                yb_query = yb_pg_queries[0]
                pg_query = yb_pg_queries[1]
                if yb_query and yb_query.execution_time_ms and pg_query and pg_query.execution_time_ms:
                    data["query_hash"].append(yb_query.query_hash)
                    data["query_tag"].append(tag)
                    tags.append(tag)
                    data["query"].append(yb_query.query)
                    data["yb_cost"].append(yb_query.execution_plan.get_estimated_cost())
                    data["yb_time"].append(yb_query.execution_time_ms)
                    data["pg_cost"].append(pg_query.execution_plan.get_estimated_cost())
                    data["pg_time"].append(pg_query.execution_time_ms)
                    yb_best = yb_query.get_best_optimization(self.config)
                    if not yb_query.compare_plans(yb_best):
                        data_yb_bad_plans["yb_cost"].append(yb_query.execution_plan.get_estimated_cost())
                        data_yb_bad_plans["yb_time"].append(yb_query.execution_time_ms)
                        data_yb_bad_plans["query_tag"].append(tag)
                    pg_best = pg_query.get_best_optimization(self.config)
                    if not pg_query.compare_plans(pg_best):
                        data_pg_bad_plans["pg_cost"].append(pg_query.execution_plan.get_estimated_cost())
                        data_pg_bad_plans["pg_time"].append(pg_query.execution_time_ms)
                        data_pg_bad_plans["query_tag"].append(tag)

        source = ColumnDataSource(data)
        source_yb_bad_plans = ColumnDataSource(data_yb_bad_plans)
        source_pg_bad_plans = ColumnDataSource(data_pg_bad_plans)

        TOOLTIPS = """
            <div style="width:200px;">
            @query
            </div>
        """
        hover_tool = HoverTool(tooltips=TOOLTIPS)
        hover_tool.renderers = []
        TOOLS = [TapTool(), BoxZoomTool(), WheelZoomTool(), PanTool(), SaveTool(), ResetTool(), hover_tool]

        tags = sorted(list(set(data['query_tag'])))

        # YB Plot
        yb_plot = figure(x_axis_label='Estimated Cost',
                         y_axis_label='Execution Time (ms)',
                         title='Yugabyte',
                         width=600, height=600,
                         tools=TOOLS, active_drag=None)

        for tag in tags:
            view = CDSView(filter=GroupFilter(column_name='query_tag', group=tag))
            # Highliht queries with bad plans
            yb_plot.scatter("yb_cost", "yb_time", size=14, line_width=4,
                            source=source_yb_bad_plans, legend_label=tag, line_color='firebrick',
                            color=None, fill_alpha=0.0, view=view)
            # Scatter plot for all queries
            yb_scatter = yb_plot.scatter("yb_cost", "yb_time", size=10, source=source,
                                         hover_color="black", legend_label=tag, view=view,
                                         color=factor_cmap('query_tag', 'Category20_20', tags),
                                         selection_color='black', nonselection_alpha=1.0)
            hover_tool.renderers.append(yb_scatter)

        # Interactive Legend
        yb_plot.legend.click_policy = 'hide'

        # Linear Regression Line
        yb_x_np = np.array(data['yb_cost'])
        yb_y_np = np.array(data['yb_time'])
        try:
            res = linregress(yb_x_np, yb_y_np)
            yb_y_data_regress = res.slope * yb_x_np + res.intercept
            yb_plot.line(x=yb_x_np, y=yb_y_data_regress)
        except ValueError:
            self.config.logger.warn(f"All x values are same. Linear regression not calculated.")

        # Tap event to jump to query
        yb_url = 'tags/@query_tag.html#@query_hash'
        yb_taptool = yb_plot.select(type=TapTool)
        yb_taptool.callback = OpenURL(url=yb_url, same_tab=True)

        # PG Plot
        pg_plot = figure(x_axis_label='Estimated Cost',
                         y_axis_label='Execution Time (ms)',
                         title='Postgres',
                         width=600, height=600,
                         tools=TOOLS, tooltips=TOOLTIPS, active_drag=None)

        for tag in tags:
            view = CDSView(filter=GroupFilter(column_name='query_tag', group=tag))
            # Highliht queries with bad plans
            pg_plot.scatter("pg_cost", "pg_time", size=14, line_width=4,
                            source=source_pg_bad_plans, legend_label=tag, line_color='firebrick',
                            color=None, fill_alpha=0.0, view=view)
            # Scatter plot for all queries
            pg_scatter = pg_plot.scatter("pg_cost", "pg_time", size=10, source=source,
                                         hover_color="black", legend_label=tag, view=view,
                                         color=factor_cmap('query_tag', 'Category20_20', tags),
                                         selection_color='black', nonselection_alpha=1.0)
            hover_tool.renderers.append(pg_scatter)

        # Interactive Legend
        pg_plot.legend.click_policy = 'hide'

        # Linear Regression Line
        pg_x_np = np.array(data['pg_cost'])
        pg_y_np = np.array(data['pg_time'])
        try:
            res = linregress(pg_x_np, pg_y_np)
            pg_y_data_regress = res.slope * pg_x_np + res.intercept
            pg_plot.line(x=pg_x_np, y=pg_y_data_regress)
        except ValueError:
            self.config.logger.warn(f"All x values are same. Linear regression not calculated.")
        # Tap event to jump to query
        pg_url = 'tags/@query_tag.html#@query_hash'
        pg_taptool = pg_plot.select(type=TapTool)
        pg_taptool.callback = OpenURL(url=pg_url, same_tab=True)

        GRIDPLOT = gridplot([[yb_plot, pg_plot]], sizing_mode='scale_both',
                            merge_tools=False)
        script, div = components(GRIDPLOT)
        return script, div

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

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms > 0],
                 [q.execution_plan.get_estimated_cost() for q in optimizations if
                  q.execution_time_ms > 0], 'k.',
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
        script, div = self.create_default_query_plots_interactive()
        self.content += f"""
++++
<script type="text/javascript" src="https://cdn.bokeh.org/bokeh/release/bokeh-3.3.0.min.js"></script>
<script type="text/javascript">
    Bokeh.set_log_level("info");
</script>
{script}
{div}
++++
"""

        self.content += "\n== QO score\n"

        yb_bests = 0
        pg_bests = 0
        qe_default_geo = []
        qe_bests_geo = []
        qo_yb_bests_geo = []
        qo_pg_bests_geo = []
        timed_out = 0
        slower_then_10x = 0
        best_slower_then_10x = 0
        inconsistent_results = 0
        total = 0

        for queries in self.queries.values():
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                inconsistent_results += 1 if yb_query.get_inconsistent_results() else 0

                pg_success = pg_query.execution_time_ms > 0

                qe_default_geo.append(yb_query.execution_time_ms / pg_query.execution_time_ms if pg_success else 1)
                qe_bests_geo.append(yb_best.execution_time_ms / pg_best.execution_time_ms if pg_success else 1)

                if yb_query.execution_time_ms > 0 and yb_best.execution_time_ms > 0:
                    qo_yb_bests_geo.append(yb_query.execution_time_ms / yb_best.execution_time_ms)
                if pg_query.execution_time_ms > 0 and pg_best.execution_time_ms > 0:
                    qo_pg_bests_geo.append(pg_query.execution_time_ms / pg_best.execution_time_ms)

                yb_bests += 1 if yb_query.compare_plans(yb_best) else 0
                pg_bests += 1 if pg_success and pg_query.compare_plans(pg_best) else 0
                timed_out += 1 if yb_query.execution_time_ms == -1 else 0
                slower_then_10x += 1 if pg_query.execution_time_ms and \
                                        (yb_query.execution_time_ms / pg_query.execution_time_ms) >= 10 else 0
                best_slower_then_10x += 1 if pg_query.execution_time_ms and \
                                             (yb_best.execution_time_ms / pg_query.execution_time_ms) >= 10 else 0

                total += 1

        self.start_table("4,1,1")
        self.content += "|Statistic|YB|PG\n"
        self.content += f"|Best execution plan picked|{'{:.2f}'.format(float(yb_bests) * 100 / total)}%" \
                        f"|{'{:.2f}'.format(float(pg_bests) * 100 / total)}%\n"
        self.content += f"|Geometric mean QE default\n2+m|{'{:.2f}'.format(self.geo_mean(qe_default_geo))}\n"
        self.content += f"|Geometric mean QE best\n2+m|{'{:.2f}'.format(self.geo_mean(qe_bests_geo))}\n"
        self.content += f"|Geometric mean QO default vs best|{'{:.2f}'.format(self.geo_mean(qo_yb_bests_geo))}" \
                        f"|{'{:.2f}'.format(self.geo_mean(qo_pg_bests_geo))}\n"
        self.content += f"|% Queries > 10x: YB default vs PG default\n" \
                        f"2+m|{slower_then_10x}/{total} (+{timed_out} timed out)\n"
        self.content += f"|% Queries > 10x: YB best vs PG default\n2+m|{best_slower_then_10x}/{total}\n"
        self.end_table()

        self.content += "\n[#top]\n== QE score\n"

        num_columns = 7
        for tag, queries in self.queries.items():
            self.start_table("1,1,1,1,1,1,4")
            self.content += "|YB|YB Best|PG|PG Best|Ratio YB vs PG|Ratio Best YB vs PG|Query\n"
            self.content += f"{num_columns}+m|{tag}.sql\n"
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                pg_success = pg_query.execution_time_ms > 0

                default_yb_equality = "[green]" if yb_query.compare_plans(yb_best) else "[red]"
                default_pg_equality = "[green]" \
                    if pg_success and pg_query.compare_plans(pg_best) else "[red]"

                best_yb_pg_equality = "(eq) " if yb_best.compare_plans(pg_best) else ""

                ratio_x3 = yb_query.execution_time_ms / (3 * pg_query.execution_time_ms) \
                    if yb_best.execution_time_ms > 0 and pg_success else 99999999
                ratio_x3_str = "{:.2f}".format(yb_query.execution_time_ms / pg_query.execution_time_ms
                                               if yb_best.execution_time_ms > 0 and pg_success else 99999999)
                ratio_color = "[green]" if ratio_x3 <= 1.0 else "[red]"

                ratio_best = yb_best.execution_time_ms / (3 * pg_best.execution_time_ms) \
                    if yb_best.execution_time_ms > 0 and pg_success else 99999999
                ratio_best_x3_str = "{:.2f}".format(yb_best.execution_time_ms / pg_best.execution_time_ms
                                                    if yb_best.execution_time_ms > 0 and pg_success else 99999999)
                ratio_best_color = "[green]" if ratio_best <= 1.0 else "[red]"

                bitmap_flag = "[blue]" \
                    if pg_success and "bitmap" in pg_query.execution_plan.full_str.lower() else "[black]"

                self.content += f"a|[black]#*{'{:.2f}'.format(yb_query.execution_time_ms)}*#\n" \
                                f"a|{default_yb_equality}#*{'{:.2f}'.format(yb_best.execution_time_ms)}*#\n" \
                                f"a|{bitmap_flag}#*{'{:.2f}'.format(pg_query.execution_time_ms)}*#\n" \
                                f"a|{default_pg_equality}#*{'{:.2f}'.format(pg_best.execution_time_ms)}*#\n" \
                                f"a|{ratio_color}#*{ratio_x3_str}*#\n" \
                                f"a|{ratio_best_color}#*{best_yb_pg_equality}{ratio_best_x3_str}*#\n"

                self.content += f"a|[#{yb_query.query_hash}_top]"
                self.append_tag_page_link(tag, yb_query.query_hash, f"Query {yb_query.query_hash}")

                self.start_source(["sql"])
                self.content += format_sql(pg_query.get_reportable_query())
                self.end_source()
                self.content += "\n"
                self.end_table_row()

            self.end_table()

        # different results links
        for tag in self.queries.keys():
            self.append_tag_page_link(tag, None, f"{tag} queries file")

        for tag, queries in self.queries.items():
            sub_report = self.create_sub_report(tag)
            sub_report.content += f"\n[#{tag}]\n== {tag} queries file\n\n"
            for query in queries:
                self.__report_query(sub_report, query[0], query[1], True)

    def __report_near_queries(self, report, query: Type[Query]):
        if query.optimizations:
            best_optimization = query.get_best_optimization(self.config)
            if add_to_report := "".join(
                    f"`{optimization.explain_hints}`\n\n"
                    for optimization in query.optimizations
                    if allowed_diff(self.config, best_optimization.execution_time_ms,
                                    optimization.execution_time_ms)):
                report.start_collapsible("Near best optimization hints")
                report.content += add_to_report
                report.end_collapsible()

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

                yb_bests += 1 if yb_query.compare_plans(yb_best) else 0
                pg_bests += 1 if pg_query.compare_plans(pg_best) else 0

                total += 1

        worksheet.write(0, 0, "YB", head_format)
        worksheet.write(0, 1, "YB Best", head_format)
        worksheet.write(0, 2, "YB EQ", head_format)
        worksheet.write(0, 3, "PG", head_format)
        worksheet.write(0, 4, "PG Best", head_format)
        worksheet.write(0, 5, "PG EQ", head_format)
        worksheet.write(0, 6, "Ratio YB vs PG", head_format)
        worksheet.write(0, 7, "Default EQ", head_format)
        worksheet.write(0, 8, "Best YB vs PG", head_format)
        worksheet.write(0, 9, "Best EQ", head_format)
        worksheet.write(0, 10, "Query", head_format)
        worksheet.write(0, 11, "Query Hash", head_format)

        row = 1
        # Iterate over the data and write it out row by row.
        for tag, queries in self.queries.items():
            for query in queries:
                yb_query: PostgresQuery = query[0]
                pg_query: PostgresQuery = query[1]

                yb_best = yb_query.get_best_optimization(self.config, )
                pg_best = pg_query.get_best_optimization(self.config, )

                default_yb_equality = yb_query.compare_plans(yb_best)
                default_pg_equality = pg_query.compare_plans(pg_best)

                default_yb_pg_equality = yb_query.compare_plans(pg_query)
                best_yb_pg_equality = yb_best.compare_plans(pg_best)

                ratio_x3 = yb_query.execution_time_ms / (3 * pg_query.execution_time_ms) \
                    if pg_query.execution_time_ms > 0 else 99999999
                ratio_x3_str = "{:.2f}".format(yb_query.execution_time_ms / pg_query.execution_time_ms
                                               if pg_query.execution_time_ms > 0 else 99999999)
                ratio_color = ratio_x3 > 1.0

                ratio_best = yb_best.execution_time_ms / (3 * pg_best.execution_time_ms) \
                    if yb_best.execution_time_ms > 0 and pg_best.execution_time_ms > 0 else 99999999
                ratio_best_x3_str = "{:.2f}".format(
                    yb_best.execution_time_ms / pg_best.execution_time_ms
                    if yb_best.execution_time_ms > 0 and pg_best.execution_time_ms > 0 else 99999999)
                ratio_best_color = ratio_best > 1.0

                bitmap_flag = pg_query.execution_plan and "bitmap" in pg_query.execution_plan.full_str.lower()

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
                worksheet.write(row, 2, default_yb_equality)
                worksheet.write(row, 3,
                                f"{'{:.2f}'.format(pg_query.execution_time_ms)}",
                                bm_format if bitmap_flag else None)
                worksheet.write(row, 4,
                                f"{'{:.2f}'.format(pg_best.execution_time_ms)}",
                                eq_format if default_pg_equality else None)
                worksheet.write(row, 5, default_pg_equality)
                worksheet.write(row, 6, f"{ratio_x3_str}", df_pf_format)
                worksheet.write(row, 7, default_yb_pg_equality)
                worksheet.write(row, 8, f"{ratio_best_x3_str}", best_pg_format)
                worksheet.write(row, 9, best_yb_pg_equality)
                worksheet.write(row, 10, f'{format_sql(pg_query.query)}')
                worksheet.write(row, 11, f'{pg_query.query_hash}')
                row += 1

        workbook.close()

    def __report_heatmap(self, report, query: Type[Query]):
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
                result += "\n".join([f"+{line}" for line_id, line in enumerate(rows)
                                     if line_id != (len(rows) - 1)]) + f"\n{rows[-1]}"
            elif row['weight'] == 0:
                result = self.fix_last_newline_in_result(result, rows)
                result += "\n".join([f"-{line}" for line_id, line in enumerate(rows)
                                     if line_id != (len(rows) - 1)]) + f"\n{rows[-1]}"
            else:
                result += f"{row['str']}"

            # skip adding extra -> to the end of list
            if row_id != last_rowid:
                result += "->"

        report.start_collapsible("Plan heatmap")
        report.start_source(["diff"])
        report.content += result
        report.end_source()
        report.end_collapsible()

    @staticmethod
    def fix_last_newline_in_result(result, rows):
        if result:
            result, last_newline = result.rsplit("\n", 1)
            rows[0] = f"{last_newline}{rows[0]}"
            result += "\n"

        return result

    # noinspection InsecureHash
    def __report_query(self, report, yb_query: Type[Query], pg_query: Type[Query], show_best: bool):
        yb_best = yb_query.get_best_optimization(self.config)
        inconsistencies = yb_query.get_inconsistent_results()

        self.reported_queries_counter += 1

        report.content += f"\n[#{yb_query.query_hash}]\n"
        report.content += f"=== Query {yb_query.query_hash}"
        report.content += f"\n{yb_query.tag}\n"
        report.append_index_page_hashtag_link("top", "Go to index")
        report.append_index_page_hashtag_link(f"{yb_query.query_hash}_top", "Show in summary")
        report.add_double_newline()

        report.start_source(["sql"])
        report.content += format_sql(yb_query.get_reportable_query())
        report.end_source()

        analyze_execution_time = extract_execution_time_from_analyze(yb_query.execution_plan.full_str)
        avg_execution_time = yb_query.execution_time_ms

        if (analyze_execution_time > avg_execution_time and
                not allowed_diff(self.config, avg_execution_time, analyze_execution_time)):
            report.add_double_newline()
            report.content += f"WARN! Analyze time is bigger than avg - `{analyze_execution_time}` > `{avg_execution_time}`"
            report.add_double_newline()

        if inconsistencies:
            report.add_double_newline()
            report.content += f"ERROR! YB Inconsistent hints - `{inconsistencies}`"
            report.add_double_newline()

        report.add_double_newline()
        report.content += f"YB Default explain hints - `{yb_query.explain_hints}`"
        report.add_double_newline()

        if show_best:
            report.add_double_newline()
            report.content += f"YB Best explain hints - `{yb_best.explain_hints}`"
            report.add_double_newline()

            self.__report_near_queries(report, yb_query)

        report.start_table("2")
        report.content += "|Default|Log scale\n"
        query_plot = self.create_query_plot(yb_best, yb_query.optimizations, yb_query)
        query_plot_log = self.create_query_plot(yb_best, yb_query.optimizations, yb_query, "log")
        report.content += f"a|image::../{query_plot}[Default,align=\"center\"]\n"
        report.content += f"a|image::../{query_plot_log}[Log scale,align=\"center\"]\n"
        report.end_table()

        report.add_double_newline()

        report.add_double_newline()
        default_yb_equality = "(eq) " if yb_query.compare_plans(yb_best) else ""
        default_pg_equality = ""
        default_yb_pg_equality = ""

        best_yb_pg_equality = ""
        if pg_query and pg_query.execution_time_ms > 0:
            report.start_table("5")
            report.content += "|Metric|YB|YB Best|PG|PG Best\n"

            pg_best = pg_query.get_best_optimization(self.config)
            default_pg_equality = "(eq) " if pg_query.compare_plans(pg_best) else ""
            best_yb_pg_equality = "(eq) " if yb_best.compare_plans(pg_best) else ""
            default_yb_pg_equality = "(eq) " if yb_query.compare_plans(pg_query) else ""

            if 'order by' in yb_query.query:
                report.start_table_row()
                report.content += f"!! Result hash" \
                                  f"|{yb_query.result_hash}" \
                                  f"|{yb_best.result_hash}" \
                                  f"|{pg_query.result_hash}" \
                                  f"|{pg_best.result_hash}" \
                    if pg_query.result_hash != yb_query.result_hash else \
                    f"Result hash" \
                    f"|`{yb_query.result_hash}" \
                    f"|{yb_best.result_hash}" \
                    f"|{pg_query.result_hash}" \
                    f"|{pg_best.result_hash}"
                report.end_table_row()

            report.start_table_row()
            report.content += f"Cardinality" \
                              f"|{yb_query.result_cardinality}" \
                              f"|{yb_best.result_cardinality}" \
                              f"|{pg_query.result_cardinality}" \
                              f"|{pg_best.result_cardinality}"
            report.end_table_row()
            report.start_table_row()
            report.content += f"Estimated cost" \
                              f"|{yb_query.execution_plan.get_estimated_cost()}" \
                              f"|{default_yb_equality}{yb_best.execution_plan.get_estimated_cost()}" \
                              f"|{pg_query.execution_plan.get_estimated_cost()}" \
                              f"|{default_pg_equality}{pg_best.execution_plan.get_estimated_cost()}"
            report.end_table_row()
            report.start_table_row()
            report.content += f"Execution time" \
                              f"|{'{:.2f}'.format(yb_query.execution_time_ms)}" \
                              f"|{default_yb_equality}{'{:.2f}'.format(yb_best.execution_time_ms)}" \
                              f"|{'{:.2f}'.format(pg_query.execution_time_ms)}" \
                              f"|{default_pg_equality}{'{:.2f}'.format(pg_best.execution_time_ms)}"
            report.end_table_row()
        else:
            report.start_table("3")
            report.content += "|Metric|YB|YB Best\n"

            if yb_best.result_hash != yb_query.result_hash:
                report.content += f"!! Result hash|{yb_query.result_hash}|{yb_best.result_hash}"
            else:
                report.content += f"Result hash|{yb_query.result_hash}|{yb_best.result_hash}"
            report.end_table_row()

            report.start_table_row()
            report.content += f"Cardinality" \
                              f"|{yb_query.result_cardinality}" \
                              f"|{yb_best.result_cardinality}"
            report.end_table_row()
            report.start_table_row()
            report.content += f"Optimizer cost" \
                              f"|{yb_query.execution_plan.get_estimated_cost()}" \
                              f"|{default_yb_equality}{yb_best.execution_plan.get_estimated_cost()}"
            report.end_table_row()
            report.start_table_row()
            report.content += f"Execution time" \
                              f"|{yb_query.execution_time_ms}" \
                              f"|{default_yb_equality}{yb_best.execution_time_ms}"
            report.end_table_row()
        report.end_table()

        report.start_table()
        report.start_table_row()

        if yb_query.query_stats:
            report.start_collapsible("YB stats default")
            report.start_source()
            report.content += str(yb_query.query_stats)
            report.end_source()
            report.end_collapsible()

        if yb_best.query_stats and not yb_query.compare_plans(yb_best):
            report.start_collapsible("YB stats best")
            report.start_source()
            report.content += str(yb_best.query_stats)
            report.end_source()
            report.end_collapsible()

        if pg_query and pg_query.execution_time_ms > 0:
            bitmap_used = "(bm) " if "bitmap" in pg_query.execution_plan.full_str.lower() else ""
            report.start_collapsible(f"{bitmap_used}PG plan")
            report.start_source(["diff"])
            report.content += pg_query.execution_plan.full_str
            report.end_source()
            report.end_collapsible()

            pg_best = pg_query.get_best_optimization(self.config)
            bitmap_used = "(bm) " if "bitmap" in pg_best.execution_plan.full_str.lower() else ""
            report.start_collapsible(f"{default_pg_equality}{bitmap_used}PG best")
            report.start_source(["diff"])
            report.content += pg_best.execution_plan.full_str
            report.end_source()
            report.end_collapsible()

            report.start_collapsible(f"{default_yb_pg_equality}PG default vs YB default")
            report.start_source(["diff"])

            # postgres plan should be red
            report.content += get_plan_diff(
                pg_query.execution_plan.full_str.replace("|", "\|"),
                yb_query.execution_plan.full_str.replace("|", "\|"),
            )
            report.end_source()
            report.end_collapsible()

            report.start_collapsible(f"{best_yb_pg_equality}PG best vs YB best")
            report.start_source(["diff"])
            report.content += get_plan_diff(
                pg_best.execution_plan.full_str.replace("|", "\|"),
                yb_best.execution_plan.full_str.replace("|", "\|"),
            )
            report.end_source()
            report.end_collapsible()

        if show_best:
            pass
            # self.__report_heatmap(report, yb_query)

        report.start_collapsible("YB default plan")
        report.start_source(["diff"])
        report.content += yb_query.execution_plan.full_str.replace("|", "\|")
        report.end_source()
        report.end_collapsible()

        report.start_collapsible(f"{default_yb_equality}YB best plan")
        report.start_source(["diff"])
        report.content += yb_best.execution_plan.full_str.replace("|", "\|")
        report.end_source()
        report.end_collapsible()

        report.content += f"{default_yb_equality}YB default vs YB best\n"
        report.start_source(["diff"])
        diff = get_plan_diff(
            yb_query.execution_plan.full_str.replace("|", "\|"),
            yb_best.execution_plan.full_str.replace("|", "\|")
        )
        if not diff:
            diff = yb_query.execution_plan.full_str.replace("|", "\|")

        report.content += diff
        report.end_source()
        report.end_table_row()

        report.content += "\n"

        report.end_table()

        report.add_double_newline()
