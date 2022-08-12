import hashlib

from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report
from utils import allowed_diff


class ApproachReport(Report):
    def __init__(self):
        super().__init__()

        self.same_execution_plan = []
        self.almost_same_execution_time = []
        self.improved_execution_time = []
        self.worse_execution_time = []

    def add_query(self, default: Query, analyze: Query, all: Query):
        if default.execution_plan == all.execution_plan:
            self.same_execution_plan.append([default, analyze, all])
        elif allowed_diff(self.config, default.execution_time_ms, all.execution_time_ms):
            self.almost_same_execution_time.append([default, analyze, all])
        elif default.execution_time_ms < all.execution_time_ms:
            self.worse_execution_time.append([default, analyze, all])
        else:
            self.improved_execution_time.append([default, analyze, all])

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.report += "\n<<worse>>\n"
        self.report += "\n<<same>>\n"
        self.report += "\n<<improved>>\n"
        self.report += "\n<<same>>\n"

        self.report += f"\n[#worse]\n== Worse execution time queries ({len(self.worse_execution_time)})\n\n"
        for query in self.worse_execution_time:
            self.__report_query(query[0], query[1], query[2])

        self.report += f"\n[#same]\n== Almost same execution time queries ({len(self.almost_same_execution_time)})\n\n"
        for query in self.almost_same_execution_time:
            self.__report_query(query[0], query[1], query[2])

        self.report += f"\n[#improved]\n== Improved execution time ({len(self.improved_execution_time)})\n\n"
        for query in self.improved_execution_time:
            self.__report_query(query[0], query[1], query[2])

        self.report += f"\n[#same]\n\n== Same execution plan ({len(self.same_execution_plan)})\n\n"
        for query in self.same_execution_plan:
            self.__report_query(query[0], query[1], query[2])

    # noinspection InsecureHash
    def __report_query(self, default: Query, analyze: Query, all: Query):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(default.query.encode('utf-8')).hexdigest()

        self.report += f"=== Query {query_hash}"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(default.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()

        self._start_table()

        self.report += "|Comparison analysis\n"

        self._start_table_row()
        self.report += f"Cost: `{default.optimizer_score}` (default) vs `{analyze.optimizer_score}` (analyze) vs `{all.optimizer_score}` (all)"
        self._end_table_row()

        self.report += "\n"

        self._start_table_row()
        self.report += f"Execution time: `{default.execution_time_ms}` (default) vs `{analyze.execution_time_ms}` (analyze) vs `{all.execution_time_ms}` (all)"
        self._end_table_row()

        self._start_table_row()

        self._start_collapsible("Default approach plan (w/o analyze)")
        self._start_source(["diff"])
        self.report += default.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Analyze approach plan (w/ analyze)")
        self._start_source(["diff"])
        self.report += analyze.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("New approach plan (w/ analyze and statistics)")
        self._start_source(["diff"])
        self.report += all.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(default.execution_plan, all.execution_plan)
        if not diff:
            diff = default.execution_plan

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
