from sql_formatter.core import format_sql

from database import Query
from tests.abstract import Report
from utils import get_md5


class RegressionReport(Report):
    def __init__(self):
        super().__init__()

        self.queries = {}

    def get_report_name(self):
        return "Regression"

    def define_version(self, first_version, second_version):
        self.report += f"[GIT COMMIT/VERSION]\n====\nFirst:\n{first_version}\n\nSecond:\n{second_version}\n====\n\n"

    def add_query(self, first_query: Query, second_query: Query):
        if first_query.tag not in self.queries:
            self.queries[first_query.tag] = [[first_query, second_query], ]
        else:
            self.queries[first_query.tag].append([first_query, second_query])

    def build_report(self):
        # link to top
        self.add_plan_comaprison()
        self.add_rpc_calls()
        self.add_rpc_wait_times()
        self.add_scanned_rows()
        self.add_peak_memory_collapsible()

        self.report += "\n[#query_summary]\n== Query Summary\n"
        num_columns = 4
        self._start_table("1,1,1,4")
        self.report += "|First|Second|Ratio (Second/First)|Query\n"
        for tag, queries in self.queries.items():
            self.report += f"{num_columns}+m|{tag}.sql\n"
            for query_id, query in enumerate(queries):
                same_plan = query[0].compare_plans(query[1].execution_plan)
                color = "[green]" if same_plan else "[orange]"
                ratio = "{:.2f}".format(
                    query[1].execution_time_ms / query[0].execution_time_ms
                    if query[0].execution_time_ms != 0 else 0)

                # insert anchor to the first query in file
                self.report += "a|"
                if query_id == 0:
                    self.report += f"[#{tag}]\n"

                # append all query stats
                self.report += f"{query[0].execution_time_ms}\n" \
                               f"|{query[1].execution_time_ms}\n" \
                               f"a|{color}#*{ratio}*#\n"
                hexdigest = get_md5(query[0].query)
                self.report += f"a|[#{hexdigest}_query]\n<<tags_summary, Go to tags summary>>\n\n<<{hexdigest}>>\n"
                self._start_source(["sql"])
                self.report += format_sql(query[1].query.replace("|", "\|"))
                self._end_source()
                self.report += "\n"
                self._end_table_row()
        self._end_table()

        for tag, queries in self.queries.items():
            self.report += f"\n== {tag} queries file\n\n"
            for query in queries:
                self.__report_query(query[0], query[1])

    def add_plan_comaprison(self):
        self._start_collapsible("Plan comparison")
        self.report += "\n[#plans_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(1 for query in queries
                                 if query[0].compare_plans(query[1].execution_plan))
            self.report += f"a|<<{tag}>>\n"
            num_changed_plans = len(queries) - num_same_plans
            color = "[green]" if num_changed_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{num_same_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_rpc_calls(self):
        self._start_collapsible("RPC Calls")
        self.report += "\n[#rpc_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(query[0].execution_plan.get_rpc_calls() != query[1].execution_plan.get_rpc_calls()
                                 for query in queries)
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if num_same_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{num_same_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_rpc_wait_times(self):
        self._start_collapsible("RPC Wait Times")
        self.report += "\n[#rpc_wait_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(query[0].execution_plan.get_rpc_wait_times() != query[1].execution_plan.get_rpc_wait_times()
                                 for query in queries)
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if num_same_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{num_same_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_scanned_rows(self):
        self._start_collapsible("Scanned rows")
        self.report += "\n[#rows_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(query[0].execution_plan.get_scanned_rows() != query[1].execution_plan.get_scanned_rows()
                                 for query in queries)
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if num_same_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{num_same_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    def add_peak_memory_collapsible(self):
        self._start_collapsible("Peak memory")
        self.report += "\n[#memory_summary]\n"
        self._start_table("2")
        for tag, queries in self.queries.items():
            num_same_plans = sum(query[0].execution_plan.get_peak_memory() != query[1].execution_plan.get_peak_memory()
                                 for query in queries)
            self.report += f"a|<<{tag}>>\n"
            color = "[green]" if num_same_plans == 0 else "[orange]"
            self.report += f"a|{color}#*{num_same_plans}*#\n"
            self._end_table_row()
        self._end_table()
        self._end_collapsible()

    # noinspection InsecureHash
    def __report_query(self, first_query: Query, second_query: Query):
        self.reported_queries_counter += 1
        query_hash = get_md5(first_query.query)

        self.report += f"\n[#{query_hash}]\n"
        self.report += f"=== Query {query_hash}"
        self.report += f"\nTags: `{first_query.tag}`\n"
        self.report += "\n<<plans_summary,Go to tags summary>>\n"
        self.report += "\n<<query_summary,Go to query summary>>\n"
        self.report += f"\n<<{query_hash}_query,Show in query summary>>\n"
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
        self.report += first_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Second version plan")
        self._start_source(["diff"])
        self.report += second_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(first_query.execution_plan.full_str, second_query.execution_plan.full_str)
        if not diff:
            diff = first_query.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
