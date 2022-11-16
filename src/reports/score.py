import os

from matplotlib import pyplot as plt
from sql_formatter.core import format_sql

from database import Query, ListOfQueries
from reports.abstract import Report
from utils import allowed_diff


class ScoreReport(Report):
    def __init__(self):
        super().__init__()

        os.mkdir(f"report/{self.start_date}/imgs")

        self.logger.info(f"Created report folder for this run at 'report/{self.start_date}'")

        self.queries = {}

    @classmethod
    def generate_report(cls, loq: ListOfQueries, pg_loq: ListOfQueries = None):
        report = ScoreReport()

        report.define_version(loq.db_version)
        report.report_model(loq.model_queries)

        for qid, query in enumerate(loq.queries):
            if not query.optimizations:
                raise AttributeError("There is no optimizations found in result file. "
                                     "Evaluate collect with --optimizations flag")

            report.add_query(query, pg_loq.queries[qid] if pg_loq else None)

        report.build_report()
        report.publish_report("taqo")

    def get_report_name(self):
        return "TAQO"

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    def calculate_score(self, query):
        if query.execution_time_ms == 0:
            return -1
        else:
            return "{:.2f}".format(
                query.get_best_optimization(self.config).execution_time_ms / query.execution_time_ms)

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

    def add_query(self, query: Query, pg: Query):
        if query.tag not in self.queries:
            self.queries[query.tag] = [[query, pg], ]
        else:
            self.queries[query.tag].append([query, pg])

    def build_report(self):
        self.report += "\n== QO score\n"

        yb_bests = 0
        pg_bests = 0
        total = 0
        for queries in self.queries.values():
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                yb_bests += 1 if yb_query.compare_plans(yb_best.execution_plan) else 0
                pg_bests += 1 if pg_query.compare_plans(pg_best.execution_plan) else 0

                total += 1

        self._start_table("4,1,1")
        self.report += "|Statistic|YB|PG\n"
        self.report += f"|Best execution plan picked|{'{:.2f}'.format(float(yb_bests) * 100 / total)}%|{'{:.2f}'.format(float(pg_bests) * 100 / total)}%\n"
        self._end_table()

        self.report += "\n[#top]\n== QE score\n"

        num_columns = 7
        for tag, queries in self.queries.items():
            self._start_table("1,1,1,1,1,1,4")
            self.report += "|YB|YB Best|PG|PG Best|Ratio YB vs PG|Best YB vs PG|Query\n"
            self.report += f"{num_columns}+m|{tag}.sql\n"
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                default_yb_equality = "(eq) " if yb_query.compare_plans(yb_best.execution_plan) else ""
                default_pg_equality = "(eq) " if pg_query.compare_plans(pg_best.execution_plan) else ""

                best_yb_pg_equality = "(eq) " if yb_best.compare_plans(pg_best.execution_plan) else ""

                ratio_x3 = yb_query.execution_time_ms / (
                        3 * pg_query.execution_time_ms) if pg_query.execution_time_ms != 0 else 99999999
                ratio_x3_str = "{:.2f}".format(yb_query.execution_time_ms / pg_query.execution_time_ms if pg_query.execution_time_ms != 0 else 99999999)
                ratio_color = "[green]" if ratio_x3 <= 1.0 else "[red]"

                ratio_best = yb_best.execution_time_ms / (
                        3 * pg_best.execution_time_ms) if yb_best.execution_time_ms != 0 else 99999999
                ratio_best_x3_str = "{:.2f}".format(yb_best.execution_time_ms / pg_best.execution_time_ms if yb_best.execution_time_ms != 0 else 99999999)
                ratio_best_color = "[green]" if ratio_best <= 1.0 else "[red]"

                bitmap_flag = "(bm) " if "bitmap" in pg_query.execution_plan.full_str.lower() else ""
                bitmap_flag_best = "(bm) " if "bitmap" in pg_best.execution_plan.full_str.lower() else ""

                self.report += f"|{'{:.2f}'.format(yb_query.execution_time_ms)}\n" \
                               f"|{default_yb_equality}{'{:.2f}'.format(yb_best.execution_time_ms)}\n" \
                               f"|{bitmap_flag}{'{:.2f}'.format(pg_query.execution_time_ms)}\n" \
                               f"|{default_pg_equality}{bitmap_flag_best}{'{:.2f}'.format(pg_best.execution_time_ms)}\n" \
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

    def __report_near_queries(self, query: Query):
        best_optimization = query.get_best_optimization(self.config)
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
    def __report_query(self, query: Query, pg_query: Query, show_best: bool):
        best_optimization = query.get_best_optimization(self.config)

        self.reported_queries_counter += 1

        self.report += f"\n[#{query.query_hash}]\n"
        self.report += f"=== Query {query.query_hash}"
        self.report += f"\n{query.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self.report += f"\n<<{query.query_hash}_top,Show in summary>>\n"
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

        self._start_table("3")
        self.report += "|Metric|Default|Best\n"
        if 'order by' in query.query:
            self._start_table_row()
            if self.config.compare_with_pg:
                self.report += \
                    f"!! Result hash|{query.result_hash}|{best_optimization.result_hash} (yb) != {pg_query.result_hash} (pg)" \
                        if pg_query.result_hash != query.result_hash else \
                        f"Result hash|`{query.result_hash}|{best_optimization.result_hash} (yb) != {pg_query.result_hash} (pg)"
            elif best_optimization.result_hash != query.result_hash:
                self.report += f"!! Result hash|{query.result_hash}|{best_optimization.result_hash}"
            else:
                self.report += f"Result hash|{query.result_hash}|{best_optimization.result_hash}"
            self._end_table_row()

        self._start_table_row()
        self.report += f"Cardinality|{query.result_cardinality}|{best_optimization.result_cardinality}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Optimizer cost|{query.optimizer_score}|{best_optimization.optimizer_score}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Execution time|{query.execution_time_ms}|{best_optimization.execution_time_ms}"
        self._end_table_row()
        self._end_table()

        self._start_table()
        self._start_table_row()

        if pg_query:
            bitmap_used = "!!! bitmap !!!" if "bitmap" in pg_query.execution_plan.full_str.lower() else ""
            self._start_collapsible(f"Postgres plan {bitmap_used}")
            self._start_source(["diff"])
            self.report += pg_query.execution_plan.full_str
            self._end_source()
            self._end_collapsible()

            self._start_collapsible("Postgres plan diff")
            self._start_source(["diff"])
            # postgres plan should be red
            self.report += self._get_plan_diff(pg_query.execution_plan.full_str,
                                               query.execution_plan.full_str, )
            self._end_source()
            self._end_collapsible()

            best_pg = pg_query.get_best_optimization(self.config)
            self._start_collapsible("Best Postgres plan")
            self._start_source(["diff"])
            self.report += best_pg.execution_plan.full_str
            self._end_source()
            self._end_collapsible()

            self._start_collapsible("Best Postgres plan diff with YB default")
            self._start_source(["diff"])
            self.report += self._get_plan_diff(best_pg.execution_plan.full_str,
                                               query.execution_plan.full_str, )
            self._end_source()
            self._end_collapsible()

            self._start_collapsible("Best Postgres plan diff with YB best")
            self._start_source(["diff"])
            self.report += self._get_plan_diff(best_pg.execution_plan.full_str,
                                               best_optimization.execution_plan.full_str, )
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
