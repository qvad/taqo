from typing import Type

from sql_formatter.core import format_sql

from objects import CollectResult, Query
from reports.abstract import Report


class RegressionXlsReport(Report):
    def __init__(self):
        super().__init__()

        self.logger.info(f"Created report folder for this run at 'report/{self.start_date}'")

        self.queries = {}

    @classmethod
    def generate_report(cls, first_loq: CollectResult, second_loq: CollectResult):
        report = RegressionXlsReport()

        for qid, query in enumerate(first_loq.queries):
            report.add_query(query, second_loq.queries[qid])

        report.build_report()

    def get_report_name(self):
        return "regression"

    def define_version(self, version):
        pass

    def add_query(self, query: Type[Query], pg: Type[Query] | None):
        if query.tag not in self.queries:
            self.queries[query.tag] = [[query, pg], ]
        else:
            self.queries[query.tag].append([query, pg])

    def build_report(self):
        import xlsxwriter

        workbook = xlsxwriter.Workbook(f'report/{self.start_date}/report_regression.xls')
        worksheet = workbook.add_worksheet()

        head_format = workbook.add_format()
        head_format.set_bold()
        head_format.set_bg_color('#999999')

        eq_format = workbook.add_format()
        eq_format.set_bold()
        eq_format.set_bg_color('#d9ead3')

        eq_bad_format = workbook.add_format()
        eq_bad_format.set_bold()
        eq_bad_format.set_bg_color('#fff2cc')

        worksheet.write(0, 0, "First", head_format)
        worksheet.write(0, 1, "Second", head_format)
        worksheet.write(0, 2, "Ratio", head_format)
        worksheet.write(0, 3, "Query", head_format)
        worksheet.write(0, 4, "Query Hash", head_format)

        row = 1
        # Iterate over the data and write it out row by row.
        for tag, queries in self.queries.items():
            for query in queries:
                first_query: Query = query[0]
                second_query: Query = query[1]

                ratio = second_query.execution_time_ms / (
                    first_query.execution_time_ms) if first_query.execution_time_ms != 0 else 99999999
                ratio_color = eq_bad_format if ratio > 1.0 else eq_format

                worksheet.write(row, 0, '{:.2f}'.format(first_query.execution_time_ms))
                worksheet.write(row, 1,
                                f"{'{:.2f}'.format(second_query.execution_time_ms)}")
                worksheet.write(row, 2, f'{ratio}', ratio_color)
                worksheet.write(row, 3, f'{format_sql(first_query.query)}')
                worksheet.write(row, 4, f'{first_query.query_hash}')
                row += 1

        workbook.close()
