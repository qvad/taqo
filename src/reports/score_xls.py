import xlsxwriter
from matplotlib import pyplot as plt
from sql_formatter.core import format_sql

from database import Query, ListOfQueries
from reports.abstract import Report


class ScoreXlsReport(Report):
    def __init__(self):
        super().__init__()

        self.logger.info(f"Created report folder for this run at 'report/{self.start_date}'")

        self.queries = {}

    @classmethod
    def generate_report(cls, loq: ListOfQueries, pg_loq: ListOfQueries = None):
        report = ScoreXlsReport()

        for qid, query in enumerate(loq.queries):
            if not query.optimizations:
                raise AttributeError("There is no optimizations found in result file. "
                                     "Evaluate collect with --optimizations flag")

            report.add_query(query, pg_loq.queries[qid] if pg_loq else None)

        report.build_report()

    def get_report_name(self):
        return "score"

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    def calculate_score(self, query):
        if query.execution_time_ms == 0:
            return -1
        else:
            return "{:.2f}".format(
                query.get_best_optimization(
                    self.config).execution_time_ms / query.execution_time_ms)

    def create_plot(self, best_optimization, optimizations, query):
        plt.xlabel('Execution time')
        plt.ylabel('Optimizer cost')

        plt.plot([q.execution_time_ms for q in optimizations if q.execution_time_ms != 0],
                 [q.optimizer_score for q in optimizations if q.execution_time_ms != 0], 'k.',
                 [query.execution_time_ms],
                 [query.optimizer_score], 'r^',
                 [best_optimization.execution_time_ms],
                 [best_optimization.optimizer_score], 'go')

        file_name = f'imgs/query_{self.reported_queries_counter}.png'
        plt.savefig(f"report/{self.start_date}/{file_name}")
        plt.close()

        return file_name

    def add_query(self, query: Query, pg: Query):
        if query.tag not in self.queries:
            self.queries[query.tag] = [[query, pg], ]
        else:
            self.queries[query.tag].append([query, pg])

    def build_report(self):
        workbook = xlsxwriter.Workbook(f'report/{self.start_date}/report_score.xls')
        worksheet = workbook.add_worksheet()

        eq_format = workbook.add_format()
        eq_format.set_bold()
        eq_format.set_bg_color('#d9ead3')

        bm_format = workbook.add_format()
        bm_format.set_bold()
        bm_format.set_bg_color('#cfe2f3')

        pg_comparison_format = workbook.add_format()
        pg_comparison_format.set_bold()
        pg_comparison_format.set_bg_color('#fce5cd')

        # Start from the first cell. Rows and columns are zero indexed.
        yb_bests = 0
        pg_bests = 0
        total = 0
        for queries in self.queries.values():
            for query in queries:
                yb_query = query[0]
                pg_query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                yb_bests += 1 if yb_query.compare_plans(yb_best.execution_plan) else 0
                pg_bests += 1 if pg_query.compare_plans(pg_best.execution_plan) else 0

                total += 1

        row = 0
        # Iterate over the data and write it out row by row.
        for tag, queries in self.queries.items():
            for query in queries:
                col = 0

                yb_query: Query = query[0]
                pg_query: Query = query[1]

                yb_best = yb_query.get_best_optimization(self.config)
                pg_best = pg_query.get_best_optimization(self.config)

                default_yb_equality = yb_query.compare_plans(yb_best.execution_plan)
                default_pg_equality = pg_query.compare_plans(pg_best.execution_plan)

                best_yb_pg_equality = yb_best.compare_plans(pg_best.execution_plan)

                ratio_x3 = yb_query.execution_time_ms / (
                        3 * pg_query.execution_time_ms) if pg_query.execution_time_ms != 0 else 99999999
                ratio_x3_str = "{:.2f}".format(
                    yb_query.execution_time_ms / pg_query.execution_time_ms if pg_query.execution_time_ms != 0 else 99999999)
                ratio_color = ratio_x3 > 1.0

                ratio_best = yb_best.execution_time_ms / (
                        3 * pg_best.execution_time_ms) if yb_best.execution_time_ms != 0 else 99999999
                ratio_best_x3_str = "{:.2f}".format(
                    yb_best.execution_time_ms / pg_best.execution_time_ms if yb_best.execution_time_ms != 0 else 99999999)
                ratio_best_color = ratio_best > 1.0

                bitmap_flag = "bitmap" in pg_query.execution_plan.full_str.lower()

                worksheet.write(row, col, '{:.2f}'.format(yb_query.execution_time_ms))
                worksheet.write(row, col + 1,
                                f"{'{:.2f}'.format(yb_best.execution_time_ms)}", eq_format if default_yb_equality else None)
                worksheet.write(row, col + 2,
                                f"{'{:.2f}'.format(pg_query.execution_time_ms)}", bm_format if bitmap_flag else None)
                worksheet.write(row, col + 3,
                                f"{'{:.2f}'.format(pg_best.execution_time_ms)}", eq_format if default_pg_equality else None)
                worksheet.write(row, col + 4, f"{ratio_x3_str}", pg_comparison_format if ratio_color else None)
                worksheet.write(row, col + 5, f"{ratio_best_x3_str}", pg_comparison_format if ratio_best_color else None)
                worksheet.write(row, col + 6, f'{format_sql(pg_query.query)}')
                worksheet.write(row, col + 7, f'{pg_query.query_hash}')
                row += 1

        workbook.close()
