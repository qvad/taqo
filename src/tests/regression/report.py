import hashlib

from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report


class RegressionReport(Report):
    def __init__(self):
        super().__init__()

        self.same_execution_plan = []
        self.improved_execution_time = []
        self.worse_execution_time = []

    def define_version(self, first_version, second_version):
        self.report += f"[VERSION]\n====\nFirst:\n{first_version}\n\nSecond:\n{second_version}\n====\n\n"

    def add_query(self, first_query: Query, second_query: Query):
        if first_query.execution_plan == second_query.execution_plan:
            self.same_execution_plan.append([first_query, second_query])
        elif first_query.execution_time_ms > second_query.execution_time_ms:
            self.improved_execution_time.append([first_query, second_query])
        else:
            self.worse_execution_time.append([first_query, second_query])

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results by analysis type\n"
        # different results links
        self.report += "\n<<worse>>\n"
        self.report += "\n<<improved>>\n"
        self.report += "\n<<same>>\n"

        self.report += "\n[#worse]\n== Worse execution time queries\n\n"
        for query in self.worse_execution_time:
            self.__report_query(query[0], query[1])

        self.report += "\n[#improved]\n== Improved execution time\n\n"
        for query in self.improved_execution_time:
            self.__report_query(query[0], query[1])

        self.report += "\n[#same]\n\n== Same execution time\n\n"
        for query in self.same_execution_plan:
            self.__report_query(query[0], query[1])

    # noinspection InsecureHash
    def __report_query(self, first_query: Query, second_query: Query):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(first_query.query.encode('utf-8')).hexdigest()

        self.report += f"=== Query {query_hash}"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(first_query.query)
        self._end_source()

        self._add_double_newline()

        self._start_execution_plan_tables()

        self.report += "|Comparison analysis\n"

        self._start_table_row()
        self.report += f"`Cost: {first_query.optimizer_score}` (first) vs `{second_query.optimizer_score}` (second)"
        self._end_table_row()

        self.report += "\n"

        self._start_table_row()
        self.report += f"`Execution time: {first_query.execution_time_ms}` (first) vs `{second_query.execution_time_ms}` (second)"
        self._end_table_row()

        self._start_table_row()

        self._start_collapsible("First version plan")
        self._start_source(["diff"])
        self.report += first_query.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Second version plan")
        self._start_source(["diff"])
        self.report += second_query.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(first_query.execution_plan, second_query.execution_plan)
        if not diff:
            diff = first_query.execution_plan

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_execution_plan_tables()

        self._add_double_newline()
