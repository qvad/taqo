import hashlib

from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report
from utils import allowed_diff, get_md5


class ApproachReport(Report):
    def __init__(self):
        super().__init__()

        self.same_execution_plan = []
        self.almost_same_execution_time = []
        self.improved_execution_time = []
        self.worse_execution_time = []

    def get_report_name(self):
        return "Default/Analyze/Analyze+Statistics"

    def add_query(self,
                  default: Query,
                  default_analyze: Query,
                  analyze: Query,
                  analyze_analyze: Query,
                  all: Query,
                  all_analyze: Query
                  ):
        queries_tuple = [default, default_analyze, analyze, analyze_analyze, all, all_analyze]
        if default.compare_plans(all_analyze.execution_plan):
            self.same_execution_plan.append(queries_tuple)
        elif allowed_diff(self.config, default.execution_time_ms, all_analyze.execution_time_ms):
            self.almost_same_execution_time.append(queries_tuple)
        elif default.execution_time_ms < all_analyze.execution_time_ms:
            self.worse_execution_time.append(queries_tuple)
        else:
            self.improved_execution_time.append(queries_tuple)

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.report += "\n<<worse>>\n"
        self.report += "\n<<same_time>>\n"
        self.report += "\n<<improved>>\n"
        self.report += "\n<<same_plan>>\n"

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
        query_hash = get_md5(default.query)

        self.report += f"=== Query {query_hash}"
        self.report += f"\n{default.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(default.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()

        self._start_table()

        self.report += "|Comparison analysis\n"

        self._start_table_row()
        self.report += f"Cost: `{default.optimizer_score}` (default) vs {default_analyze.optimizer_score}` (default analyze) vs " \
                       f"`{analyze.optimizer_score}` (table analyze) vs {analyze_analyze.optimizer_score}` (table analyze + query analyze) vs " \
                       f"`{all.optimizer_score}` (stats + table analyze) vs {all_analyze.optimizer_score}` (stats + table analyze + query analyze)"
        self._end_table_row()

        self.report += "\n"

        self._start_table_row()
        self.report += f"Execution time: `{default.execution_time_ms}` (default) vs {default_analyze.execution_time_ms}` (default analyze) vs " \
                       f"`{analyze.execution_time_ms}` (table analyze) vs {analyze_analyze.execution_time_ms}` (table analyze + query analyze) vs " \
                       f"`{all.execution_time_ms}` (stats + table analyze) vs {all_analyze.optimizer_score}` (stats + table analyze + query analyze)"
        self._end_table_row()

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

        self._start_collapsible("Stats + table analyze with EXPLAIN ANALYZE (w/ analyze and statistics)")
        self._start_source(["diff"])
        self.report += all_analyze.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(default.execution_plan.full_str, all_analyze.execution_plan.full_str)
        if not diff:
            diff = default.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
