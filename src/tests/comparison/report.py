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
        self.report += "\n[#top]\n== Summary\n"

        num_columns = 5
        self._start_table("1,1,1,1,4")
        self.report += "|Yugabyte|Postgres|Ratio vs Postgres|Ratio vs Postgres x3|Query\n"
        for tag, queries in self.queries.items():
            self.report += f"{num_columns}+m|{tag}.sql\n"
            for query in queries:
                ratio = "{:.2f}".format(query[0].execution_time_ms / query[1].execution_time_ms if query[1].execution_time_ms != 0 else 0)
                ratio_x3 = query[0].execution_time_ms / (3 * query[1].execution_time_ms) if query[1].execution_time_ms != 0 else 0
                ratio_x3_str = "{:.2f}".format(query[0].execution_time_ms / (3 * query[1].execution_time_ms) if query[1].execution_time_ms != 0 else 0)
                color = "[green]" if ratio_x3 <= 1.0 else "[red]"
                self.report += f"|{query[0].execution_time_ms}\n" \
                               f"|{query[1].execution_time_ms}\n" \
                               f"a|*{ratio}*\n" \
                               f"a|{color}#*{ratio_x3_str}*#\n"
                hexdigest = hashlib.md5(query[0].query.encode('utf-8')).hexdigest()
                self.report += f"a|[#{hexdigest}_top]\n<<{hexdigest}>>\n"
                self._start_source(["sql"])
                self.report += format_sql(query[1].query.replace("|", "\|"))
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
                self.__report_query(query[0], query[1])

    # noinspection InsecureHash
    def __report_query(self, yb_query: Query, pg_query: Query):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(yb_query.query.encode('utf-8')).hexdigest()

        self.report += f"\n[#{query_hash}]\n"
        self.report += f"=== Query {query_hash}"
        self.report += f"\n{yb_query.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self.report += f"\n<<{query_hash}_top,Show in summary>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(yb_query.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()

        self._start_table()

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

        self._end_table()

        self._add_double_newline()
