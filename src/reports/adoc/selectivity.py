from sql_formatter.core import format_sql

from objects import CollectResult, Query
from reports.abstract import Report
from utils import allowed_diff


class SelectivityReport(Report):
    def __init__(self):
        super().__init__()

        self.different_explain_plans = []
        self.same_execution_plan = []
        self.almost_same_execution_time = []
        self.improved_execution_time = []
        self.worse_execution_time = []

    def get_report_name(self):
        return "Default/Analyze/Analyze+Statistics"

    @classmethod
    def generate_report(cls,
                        loq_default: CollectResult,
                        loq_default_analyze: CollectResult,
                        loq_ta: CollectResult,
                        loq_ta_analyze: CollectResult,
                        loq_stats: CollectResult,
                        loq_stats_analyze: CollectResult):
        report = SelectivityReport()

        report.report_model(loq_default.model_queries)

        for query in zip(loq_default.queries,
                         loq_default_analyze.queries,
                         loq_ta.queries,
                         loq_ta_analyze.queries,
                         loq_stats.queries,
                         loq_stats_analyze.queries):
            report.add_query(*query)

        report.build_report()
        report.publish_report("sltvty")

    def add_query(self,
                  default: Query,
                  default_analyze: Query,
                  ta: Query,
                  ta_analyze: Query,
                  stats: Query,
                  stats_analyze: Query
                  ):
        queries_tuple = [default, default_analyze, ta, ta_analyze, stats, stats_analyze]
        if not default.compare_plans(default_analyze.execution_plan) or \
                not ta.compare_plans(ta_analyze.execution_plan) or \
                not stats.compare_plans(stats_analyze.execution_plan):
            self.different_explain_plans.append(queries_tuple)

        if default.compare_plans(stats_analyze.execution_plan):
            self.same_execution_plan.append(queries_tuple)
        elif allowed_diff(self.config, default.execution_time_ms, stats_analyze.execution_time_ms):
            self.almost_same_execution_time.append(queries_tuple)
        elif default.execution_time_ms < stats_analyze.execution_time_ms:
            self.worse_execution_time.append(queries_tuple)
        else:
            self.improved_execution_time.append(queries_tuple)

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.report += "\n<<error>>\n"
        self.report += "\n<<worse>>\n"
        self.report += "\n<<same_time>>\n"
        self.report += "\n<<improved>>\n"
        self.report += "\n<<same_plan>>\n"

        self.report += f"\n[#error]\n== ERROR: Different EXPLAIN and EXPLAIN ANALYZE plans ({len(self.different_explain_plans)})\n\n"
        for query in self.different_explain_plans:
            self.__report_query(*query)

        self.report += f"\n[#worse]\n== Worse execution time queries ({len(self.worse_execution_time)})\n\n"
        for query in self.worse_execution_time:
            self.__report_query(*query)

        self.report += f"\n[#same_time]\n== Almost same execution time queries ({len(self.almost_same_execution_time)})\n\n"
        for query in self.almost_same_execution_time:
            self.__report_query(*query)

        self.report += f"\n[#improved]\n== Improved execution time ({len(self.improved_execution_time)})\n\n"
        for query in self.improved_execution_time:
            self.__report_query(*query)

        self.report += f"\n[#same_plan]\n\n== Same execution plan ({len(self.same_execution_plan)})\n\n"
        for query in self.same_execution_plan:
            self.__report_query(*query)

    # noinspection InsecureHash
    def __report_query(self,
                       default: Query,
                       default_analyze: Query,
                       analyze: Query,
                       analyze_analyze: Query,
                       all: Query,
                       all_analyze: Query):
        self.reported_queries_counter += 1

        self.report += f"=== Query {default.query_hash}"
        self.report += f"\n{default.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(default.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()

        self._start_table("7")
        self.report += "|Metric|Default|Default+QA|TA|TA + QA|S+TA|S+TA+QA\n"
        self._start_table_row()
        self.report += f"Cardinality|{default.result_cardinality}|{default_analyze.result_cardinality}|" \
                       f"{analyze.result_cardinality}|{analyze_analyze.result_cardinality}|" \
                       f"{all.result_cardinality}|{all_analyze.result_cardinality}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Optimizer cost|{default.execution_plan.get_estimated_cost()}|{default_analyze.execution_plan.get_estimated_cost()}|" \
                       f"{analyze.execution_plan.get_estimated_cost()}|{analyze_analyze.execution_plan.get_estimated_cost()}|" \
                       f"{all.execution_plan.get_estimated_cost()}|{all_analyze.execution_plan.get_estimated_cost()}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Execution time|{default.execution_time_ms}|{default_analyze.execution_time_ms}|" \
                       f"{analyze.execution_time_ms}|{analyze_analyze.execution_time_ms}|" \
                       f"{all.execution_time_ms}|{all_analyze.execution_time_ms}"
        self._end_table_row()
        self._end_table()

        self._start_table()

        self._start_table_row()

        self._start_collapsible("Default approach plan (w/o analyze)")
        self._start_source(["diff"])
        self.report += default.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Default approach plan with EXPLAIN ANALYZE (w/o analyze)")
        self._start_source(["diff"])
        self.report += default_analyze.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Plan with analyzed table (w/ analyze)")
        self._start_source(["diff"])
        self.report += analyze.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Plan with analyzed table with EXPLAIN ANALYZE (w/ analyze)")
        self._start_source(["diff"])
        self.report += analyze_analyze.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Stats + table analyze (w/ analyze and statistics)")
        self._start_source(["diff"])
        self.report += all.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible(
            "Stats + table analyze with EXPLAIN ANALYZE (w/ analyze and statistics)")
        self._start_source(["diff"])
        self.report += all_analyze.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(
            default.execution_plan.full_str,
            all_analyze.execution_plan.full_str
        )
        if not diff:
            diff = default.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
