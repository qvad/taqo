import hashlib

from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report


class RegressionReport(Report):
    def __init__(self):
        super().__init__()

        self.queries = {}

    def define_version(self, first_version, second_version):
        self.report += f"[VERSION]\n====\nFirst:\n{first_version}\n\nSecond:\n{second_version}\n====\n\n"

    def add_query(self, first_query: Query, second_query: Query):
        if first_query.tag not in self.queries:
            self.queries[first_query.tag] = [[first_query, second_query], ]
        else:
            self.queries[first_query.tag].append([first_query, second_query])

    def build_report(self):
        # link to top
        self.report += "\n[#top]\n== Summary\n"

        num_columns = 4
        self._start_table("1,1,1,4")
        self.report += "|First|Second|Ratio|Query\n"
        for tag, queries in self.queries.items():
            self.report += f"{num_columns}+m|{tag}.sql\n"
            for query in queries:
                same_plan = query[0].compare_plans(query[1].execution_plan)
                color = "[green]" if same_plan else "[orange]"
                ratio = "{:.2f}".format(query[0].execution_time_ms / query[1].execution_time_ms if query[1].execution_time_ms != 0 else 0)
                self.report += f"|{query[0].execution_time_ms}\n" \
                               f"|{query[1].execution_time_ms}\n" \
                               f"a|{color}#*{ratio}*#\n"
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
    def __report_query(self, first_query: Query, second_query: Query):
        self.reported_queries_counter += 1
        query_hash = hashlib.md5(first_query.query.encode('utf-8')).hexdigest()

        self.report += f"\n[#{query_hash}]\n"
        self.report += f"=== Query {query_hash}"
        self.report += f"\n{first_query.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self.report += f"\n<<{query_hash}_top,Show in summary>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(first_query.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()

        self._start_table()

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

        self._end_table()

        self._add_double_newline()
