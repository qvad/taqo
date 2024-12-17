import ast
import json
from typing import Type

from actions.report import AbstractReportAction
from collect import CollectResult
from objects import Query


class ScoreStatsReport(AbstractReportAction):

    def __init__(self):
        super().__init__(False)

        self.queries = {}
        self.json = {}

    @classmethod
    def generate_report(cls, loq: CollectResult, pg_loq: CollectResult = None):
        report = ScoreStatsReport()

        for query in loq.queries:
            report.add_query(query, pg_loq.find_query_by_hash(query.query_hash) if pg_loq else None)

        report.build_report(loq)
        report.dump_json()

    def add_query(self, query: Type[Query], pg: Type[Query] | None):
        if query.tag not in self.queries:
            self.queries[query.tag] = [[query, pg], ]
        else:
            self.queries[query.tag].append([query, pg])

    def build_report(self, loq):
        yb_bests = 0
        pg_bests = 0
        qe_default_geo = []
        qe_bests_geo = []
        qo_yb_bests_geo = []
        qo_pg_bests_geo = []
        timed_out = 0
        slower_then_10x = 0
        best_slower_then_10x = 0
        inconsistent_results = 0
        total = 0

        try:
            for queries in self.queries.values():
                for query in queries:
                    try:
                        yb_query = query[0]
                        pg_query = query[1]

                        yb_best = yb_query.get_best_optimization(self.config)
                        pg_best = pg_query.get_best_optimization(self.config)

                        inconsistent_results += 1 if yb_query.get_inconsistent_results() else 0

                        pg_success = pg_query.execution_time_ms > 0
                        yb_success = yb_query.execution_time_ms > 0

                        qe_default_geo.append(yb_query.execution_time_ms / pg_query.execution_time_ms
                                              if pg_success and yb_success else 1)
                        qe_bests_geo.append(yb_best.execution_time_ms / pg_best.execution_time_ms
                                            if pg_success and yb_success else 1)

                        if yb_query.execution_time_ms > 0 and yb_best.execution_time_ms > 0:
                            qo_yb_bests_geo.append(yb_query.execution_time_ms / yb_best.execution_time_ms)
                        if pg_query.execution_time_ms > 0 and pg_best.execution_time_ms > 0:
                            qo_pg_bests_geo.append(pg_query.execution_time_ms / pg_best.execution_time_ms)

                        yb_bests += 1 if yb_query.compare_plans(yb_best) else 0
                        pg_bests += 1 if pg_success and pg_query.compare_plans(pg_best) else 0
                        timed_out += 1 if yb_query.execution_time_ms == -1 else 0
                        slower_then_10x += 1 if pg_query.execution_time_ms and \
                                                (yb_query.execution_time_ms / pg_query.execution_time_ms) >= 10 else 0
                        best_slower_then_10x += 1 if pg_query.execution_time_ms and \
                                                     (yb_best.execution_time_ms / pg_query.execution_time_ms) >= 10 else 0
                    except Exception as e:
                        pass

                    total += 1
        except Exception as e:
            self.logger.exception(e)

        self.json = {
            "best_picked": '{:.2f}'.format(float(yb_bests) * 100 / total),
            "qe_default": '{:.2f}'.format(self.geo_mean(qe_default_geo)),
            "qe_best": '{:.2f}'.format(self.geo_mean(qe_bests_geo)),
            "qo_default_vs_best": '{:.2f}'.format(self.geo_mean(qo_yb_bests_geo)),

            "total": total,
            "timeout": timed_out,
            "more_10x_default_vs_default": slower_then_10x,
            "more_10x_best_vs_default": best_slower_then_10x,

            "version": loq.db_version,
            "commit": loq.git_message,
            "ddl_time": loq.ddl_execution_time,
            "is_server_side": ast.literal_eval(loq.config.replace("''", "'")).get("server_side_execution", False),
            "model_time": loq.model_execution_time,
        }

    def dump_json(self):
        self.logger.info(f"Result: {json.dumps(self.json)}")
