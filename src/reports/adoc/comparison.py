from sql_formatter.core import format_sql

from objects import CollectResult, Query
from reports.abstract import Report


class ComparisonReport(Report):
    def __init__(self):
        super().__init__()

        self.queries = {}

    @classmethod
    def generate_report(cls,
                        loq_yb: CollectResult,
                        loq_pg: CollectResult):
        report = ComparisonReport()

        report.define_version(loq_yb.db_version, loq_pg.db_version)
        report.report_model(loq_yb.model_queries)
        report.report_config(loq_yb.config, "YB")
        report.report_config(loq_pg.config, "PG")

        for query in zip(loq_yb.queries, loq_pg.queries):
            report.add_query(*query)

        report.build_report()
        report.publish_report("cmp")

    def get_report_name(self):
        return "Comparison"

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
                ratio = "{:.2f}".format(query[0].execution_time_ms / query[1].execution_time_ms if query[1].execution_time_ms != 0 else 99999999)
                ratio_x3 = query[0].execution_time_ms / (3 * query[1].execution_time_ms) if query[1].execution_time_ms != 0 else 99999999
                ratio_x3_str = "{:.2f}".format(query[0].execution_time_ms / (3 * query[1].execution_time_ms) if query[1].execution_time_ms != 0 else 99999999)
                color = "[green]" if ratio_x3 <= 1.0 else "[red]"
                self.report += f"|{query[0].execution_time_ms}\n" \
                               f"|{query[1].execution_time_ms}\n" \
                               f"a|*{ratio}*\n" \
                               f"a|{color}#*{ratio_x3_str}*#\n"
                self.report += f"a|[#{query[0].query_hash}_top]\n<<{query[0].query_hash}>>\n"
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

        self.report += f"\n[#{yb_query.query_hash}]\n"
        self.report += f"=== Query {yb_query.query_hash}"
        self.report += f"\n{yb_query.tag}\n"
        self.report += "\n<<top,Go to top>>\n"
        self.report += f"\n<<{yb_query.query_hash}_top,Show in summary>>\n"
        self._add_double_newline()

        self._start_source(["sql"])
        self.report += format_sql(yb_query.query.replace("|", "\|"))
        self._end_source()

        self._add_double_newline()

        self._start_table("3")
        self.report += "|Metric|Yugabyte|Postgres\n"
        self._start_table_row()
        self.report += f"Cardinality|{yb_query.result_cardinality}|{pg_query.result_cardinality}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Estimated cost|{yb_query.execution_plan.get_estimated_cost()}|{pg_query.execution_plan.get_estimated_cost()}"
        self._end_table_row()
        self._start_table_row()
        self.report += f"Execution time|{yb_query.execution_time_ms}|{pg_query.execution_time_ms}"
        self._end_table_row()
        self._end_table()

        self._start_table()
        self._start_table_row()

        self._start_collapsible("Yugabyte version plan")
        self._start_source(["diff"])
        self.report += yb_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_collapsible("Postgres version plan")
        self._start_source(["diff"])
        self.report += pg_query.execution_plan.full_str
        self._end_source()
        self._end_collapsible()

        self._start_source(["diff"])

        diff = self._get_plan_diff(
            pg_query.execution_plan.full_str,
            yb_query.execution_plan.full_str,
        )
        if not diff:
            diff = yb_query.execution_plan.full_str

        self.report += diff
        self._end_source()
        self._end_table_row()

        self.report += "\n"

        self._end_table()

        self._add_double_newline()
