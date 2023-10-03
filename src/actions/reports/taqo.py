from typing import Type

from matplotlib import pyplot as plt
from sql_formatter.core import format_sql

from collect import CollectResult
from objects import Query
from actions.report import AbstractReportAction
from utils import allowed_diff, get_plan_diff


class TaqoReport(AbstractReportAction):
    def __init__(self):
        super().__init__()

        self.logger.info(f"Created report folder for this run at 'report/{self.start_date}'")

        self.failed_validation = []
        self.same_execution_plan = []
        self.better_plan_found = []

    @classmethod
    def generate_report(cls, loq: CollectResult, pg_loq: CollectResult = None):
        report = TaqoReport()

        report.define_version(loq.db_version)
        report.report_config(loq.config, "YB")
        if pg_loq:
            report.report_config(pg_loq.config, "PG")
        report.report_model(loq.model_queries)

        for qid, query in enumerate(loq.queries):
            report.add_query(query, pg_loq.queries[qid] if pg_loq else None)

        report.build_report()
        report.publish_report("taqo")

    def get_report_name(self):
        return "TAQO"

    def define_version(self, version):
        self.content += f"[VERSION]\n====\n{version}\n====\n\n"

    def calculate_score(self, query):
        if query.execution_time_ms == 0:
            return -1
        else:
            return "{:.2f}".format(
                query.get_best_optimization(self.config, ).execution_time_ms / query.execution_time_ms)

    def create_plot(self, best_optimization, optimizations, query):
        plt.xlabel('Execution time')
        plt.ylabel('Optimizer cost')

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms > 0],
                 [q.execution_plan.get_estimated_cost() for q in optimizations if q.execution_time_ms > 0], 'k.',
                 [query.execution_time_ms],
                 [query.execution_plan.get_estimated_cost()], 'r^',
                 [best_optimization.execution_time_ms],
                 [best_optimization.execution_plan.get_estimated_cost()], 'go')

        file_name = f'imgs/query_{self.reported_queries_counter}.png'
        plt.savefig(f"report/{self.start_date}/{file_name}")
        plt.close()

        return file_name

    def add_query(self, query: Type[Query], pg: Type[Query] | None):
        best_optimization = query.get_best_optimization(self.config)

        if len(query.optimizations) > 1:
            if self.config.compare_with_pg and query.result_hash != pg.result_hash:
                self.failed_validation.append([query, pg])
            if not self.config.compare_with_pg and query.result_hash != best_optimization.result_hash:
                self.failed_validation.append([query, pg])

            if allowed_diff(self.config, query.execution_time_ms,
                            best_optimization.execution_time_ms):
                self.same_execution_plan.append([query, pg])
            else:
                self.better_plan_found.append([query, pg])
        else:
            self.same_execution_plan.append([query, pg])

    def build_report(self):
        # link to top
        self.content += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.content += "\n<<result>>\n"
        self.content += "\n<<better>>\n"
        self.content += "\n<<found>>\n"

        self.content += f"\n[#result]\n== Result validation failure ({len(self.failed_validation)})\n\n"
        for query in self.failed_validation:
            self.__report_query(query[0], query[1], True)

        self.content += f"\n[#better]\n== Better plan found queries ({len(self.better_plan_found)})\n\n"
        for query in self.better_plan_found:
            self.__report_query(query[0], query[1], True)

        self.content += f"\n[#found]\n== No better plan found ({len(self.same_execution_plan)})\n\n"
        for query in self.same_execution_plan:
            self.__report_query(query[0], query[1], False)

    def __report_near_queries(self, query: Query):
        best_optimization = query.get_best_optimization(self.config)
        if add_to_report := "".join(
                f"`{optimization.explain_hints}`\n\n"
                for optimization in query.optimizations
                if allowed_diff(self.config, best_optimization.execution_time_ms,
                                optimization.execution_time_ms)):
            self.start_collapsible("All best optimization hints")
            self.content += add_to_report
            self.end_collapsible()

    def __report_heatmap(self, query: Query):
        """
        Here is the deal. In PG plans we can separate each plan tree node by splitting by `->`
        When constructing heatmap need to add + or - to the beginning of string `\n`.
        So there is 2 splitters - \n and -> and need to construct correct result.

        :param query:
        :return:
        """
        execution_plan_heatmap = query.heatmap()
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

        self.start_collapsible("Plan heatmap")
        self.start_source(["diff"])
        self.content += result
        self.end_source()
        self.end_collapsible()

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

        self.content += f"=== Query {query.query_hash} " \
                        f"(Optimizer efficiency - {self.calculate_score(query)})"
        self.content += "\n<<top,Go to top>>\n"
        self.add_double_newline()

        self.start_source(["sql"])
        self.content += format_sql(query.query.replace("|", "\|"))
        self.end_source()

        self.add_double_newline()
        self.content += f"Default explain hints - `{query.explain_hints}`"
        self.add_double_newline()

        if show_best:
            self.add_double_newline()
            self.content += f"Better explain hints - `{best_optimization.explain_hints}`"
            self.add_double_newline()

            self.__report_near_queries(query)

        filename = self.create_plot(best_optimization, query.optimizations, query)
        self.content += f"image::{filename}[\"Query {self.reported_queries_counter}\"]"

        self.add_double_newline()

        self.start_table("3")
        self.content += "|Metric|Default|Best\n"
        if 'order by' in query.query:
            self.start_table_row()
            if self.config.compare_with_pg:
                self.content += \
                    f"!! Result hash|{query.result_hash}|{best_optimization.result_hash} (yb) != {pg_query.result_hash} (pg)" \
                        if pg_query.result_hash != query.result_hash else \
                        f"Result hash|`{query.result_hash}|{best_optimization.result_hash} (yb) != {pg_query.result_hash} (pg)"
            elif best_optimization.result_hash != query.result_hash:
                self.content += f"!! Result hash|{query.result_hash}|{best_optimization.result_hash}"
            else:
                self.content += f"Result hash|{query.result_hash}|{best_optimization.result_hash}"
            self.end_table_row()

        self.start_table_row()
        self.content += f"Cardinality|{query.result_cardinality}|{best_optimization.result_cardinality}"
        self.end_table_row()
        self.start_table_row()
        self.content += f"Optimizer cost|{query.execution_plan.get_estimated_cost()}|{best_optimization.execution_plan.get_estimated_cost()}"
        self.end_table_row()
        self.start_table_row()
        self.content += f"Execution time|{query.execution_time_ms}|{best_optimization.execution_time_ms}"
        self.end_table_row()
        self.end_table()

        self.start_table()
        self.start_table_row()

        if pg_query:
            bitmap_used = "!!! bitmap !!!" if "bitmap" in pg_query.execution_plan.full_str.lower() else ""
            self.start_collapsible(f"Postgres plan {bitmap_used}")
            self.start_source(["diff"])
            self.content += pg_query.execution_plan.full_str
            self.end_source()
            self.end_collapsible()

            self.start_collapsible("Postgres plan diff")
            self.start_source(["diff"])
            # postgres plan should be red
            self.content += get_plan_diff(pg_query.execution_plan.full_str,
                                          query.execution_plan.full_str, )
            self.end_source()
            self.end_collapsible()

            best_pg = pg_query.get_best_optimization(self.config)
            self.start_collapsible("Best Postgres plan")
            self.start_source(["diff"])
            self.content += best_pg.execution_plan.full_str
            self.end_source()
            self.end_collapsible()

            self.start_collapsible("Best Postgres plan diff with YB default")
            self.start_source(["diff"])
            self.content += get_plan_diff(
                query.execution_plan.full_str,
                best_pg.execution_plan.full_str,
            )
            self.end_source()
            self.end_collapsible()

            self.start_collapsible("Best Postgres plan diff with YB best")
            self.start_source(["diff"])
            self.content += get_plan_diff(
                best_pg.execution_plan.full_str,
                best_optimization.execution_plan.full_str,
            )
            self.end_source()
            self.end_collapsible()

        if show_best:
            self.__report_heatmap(query)

        self.start_collapsible("Original plan")
        self.start_source(["diff"])
        self.content += query.execution_plan.full_str
        self.end_source()
        self.end_collapsible()

        self.start_collapsible("Best plan")
        self.start_source(["diff"])
        self.content += best_optimization.execution_plan.full_str
        self.end_source()
        self.end_collapsible()

        self.start_source(["diff"])

        diff = get_plan_diff(
            query.execution_plan.full_str,
            best_optimization.execution_plan.full_str
        )
        if not diff:
            diff = query.execution_plan.full_str

        self.content += diff
        self.end_source()
        self.end_table_row()

        self.content += "\n"

        self.end_table()

        self.add_double_newline()
