import hashlib

from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report


class ComparisonReport(Report):
    def __init__(self):
        super().__init__()

        self.queries = {}

    def define_version(self, first_version, second_version):
        self.report += f"[VERSION]\n====\nYugabyte:\n{first_version}\n\nPostgres:\n{second_version}\n====\n\n"

    def add_query(self, first_query: Query, second_query: Query):
        if first_query.tag not in self.queries:
            self.queries[first_query.tag] = [[first_query, second_query], ]
        else:
            self.queries[first_query.tag].append([first_query, second_query])

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== All results for files\n"
        # different results links
        for tag in self.queries.keys():
            self.report += f"\n<<{tag}>>\n"

        for tag, queries in self.queries.items():
            self.report += f"\n[#{tag}]\n== {tag} queries file\n\n"
            for query in queries:
                self.__report_query(query[0], query[1])

    # noinspection InsecureHash
    def __report_query(self, yb_query: Query, pg_query: Query):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(yb_query.query.encode('utf-8')).hexdigest()

        self.report += f"=== Query {query_hash}"
        self.report += "\n<<top,Go to top>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(yb_query.query)
        self._end_source()

        self._add_double_newline()

        self._start_execution_plan_tables()

        self.report += "|Comparison analysis\n"

        self._start_table_row()
        self.report += f"`Cost: {yb_query.optimizer_score}` (yb) vs `{pg_query.optimizer_score}` (pg)"
        self._end_table_row()

        self.report += "\n"

        self._start_table_row()
        self.report += f"`Execution time: {yb_query.execution_time_ms}` (yb) vs `{pg_query.execution_time_ms}` (pg)"
        self._end_table_row()

        self._start_table_row()

        self._start_collapsible("Yugabyte version plan")
        self._start_source(["diff"])
        self.report += yb_query.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Postgres version plan")
        self._start_source(["diff"])
        self.report += pg_query.execution_plan
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(yb_query.execution_plan, pg_query.execution_plan)
        if not diff:
            diff = yb_query.execution_plan

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_execution_plan_tables()

        self._add_double_newline()