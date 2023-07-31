import re
from collections import namedtuple
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import chain
from operator import itemgetter
from typing import Type

from matplotlib import pyplot as plt
from matplotlib import rcParams

from collect import CollectResult
from objects import Query, ScanNode, PlanNodeVisitor
from actions.report import AbstractReportAction


PlotSeriesData = namedtuple('PlotSeriesData', ['fmt', 'x', 'cost', 'time_ms'])


@dataclass
class ChartOptions:
    adjust_cost_by_actual_rows: bool = False
    multipy_by_nloops: bool = False

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))

@dataclass
class ChartSetSpec:
    title: str
    xlabel: str
    ylabel1: str
    ylabel2: str
    query_filter: Callable[[str],bool]
    node_filter: Callable[[], bool]
    x_getter: Callable
    series_label_suffix: Callable = None
    options: ChartOptions = field(default_factory=ChartOptions)

    file_names: [str] = field(default_factory=list)
    queries: (str) = field(default_factory=set)
    plot_series: {str: list[str] } = field(default_factory=dict)
    plot_series_data: { str: list[PlotSeriesData] } = field(default_factory=dict)


@dataclass
class PlanFeatures:
    is_single_table: bool = False
    has_key_access_index: bool = False
    has_scan_filter_index: bool = False
    has_tfbr_filter_index: bool = False
    has_no_filter_index: bool = False
    has_table_filter_seqscan: bool = False
    has_local_filter: bool = False

    # these need to be computed at the end
    has_single_scan_node: bool = False
    has_no_condition_scan: bool = False

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))

    def merge(self, other):
        for a in self.__dict__.keys():
            if getattr(other, a):
                setattr(self, a, True)


class PlanNodeCollectorContext:
    def __init__(self, plan_features):
        self.seq_scan_nodes: {str: list[ScanNode]} = {}
        self.any_index_scan_nodes: {str: list[ScanNode]} = {}
        self.pf = plan_features

    def __str__(self):
        s = ''
        for t, nodes in chain(self.seq_scan_nodes.items(),
                              self.any_index_scan_nodes.items()):
            s += f'  {t}: {len(nodes)} nodes'
            for n in nodes:
                s += f'    {n.get_full_str()}'
        s += f' plan_features: [{self.pf}]'
        return s


class PlanNodeCollector(PlanNodeVisitor):
    def __init__(self, ctx, logger):
        super().__init__()
        self.ctx = ctx
        self.logger = logger
        self.num_scans = 0
        self.depth = 0

    def __enter(self):
        self.ctx.pf.__init__()

    def __exit(self):
        self.ctx.pf.has_single_scan_node = (self.num_scans == 1)
        self.ctx.pf.has_no_condition_scan = (not self.ctx.pf.has_key_access_index
                                             and not self.ctx.pf.has_scan_filter_index
                                             and not self.ctx.pf.has_tfbr_filter_index
                                             and not self.ctx.pf.has_table_filter_seqscan
                                             and not self.ctx.pf.has_local_filter)

    def visit_PlanNode(self, node):
        if self.depth == 0:
            self.__enter()
        self.depth += 1

        self.generic_visit(node)

        self.depth -= 1
        if self.depth == 0:
            self.__exit()

    def visit_ScanNode(self, node):
        if self.depth == 0:
            self.__enter()
        self.depth += 1
        self.num_scans += 1

        if int(node.nloops) > 0:
            table = node.table_alias or node.table_name
            self.ctx.pf.has_local_filter = int(node.get_local_filter() != None)
            if node.is_seq_scan:
                if table not in self.ctx.seq_scan_nodes:
                    self.ctx.seq_scan_nodes[table] = []
                self.ctx.seq_scan_nodes[table].append(node)
                self.ctx.pf.has_table_filter_seqscan |= int(node.get_remote_filter() != None)
            elif node.is_any_index_scan:
                if table not in self.ctx.any_index_scan_nodes:
                    self.ctx.any_index_scan_nodes[table] = []
                self.ctx.any_index_scan_nodes[table].append(node)
                self.ctx.pf.has_key_access_index |= int(node.get_index_cond() != None)
                self.ctx.pf.has_scan_filter_index |= int(node.get_remote_filter() != None)
                self.ctx.pf.has_tfbr_filter_index |= int(node.get_remote_tfbr_filter() != None)
                self.ctx.pf.has_no_filter_index |= (not self.ctx.pf.has_scan_filter_index
                                                    and not self.ctx.pf.has_tfbr_filter_index
                                                    and not self.ctx.pf.has_local_filter)
            else:
                self.logger.warn(f'Unknown ScanNode: node_type={node.node_type}')

        self.generic_visit(node)

        self.depth -= 1
        if self.depth == 0:
            self.__exit()


class CostReport(AbstractReportAction):
    def __init__(self):
        super().__init__()

        self.report_location = f'report/{self.start_date}'
        self.image_folder = 'imgs'

        self.table_row_map: { str: float } = {}
        self.node_projection_width_map: { str: int } = {}
        self.scan_node_map: { str: { str: [ ScanNode ] } } = {}
        self.query_map: { str: (Query, PlanFeatures) } = {}

        self.num_no_opt_queries = 0


    def get_image_path(self, file_name):
        return f'{self.report_location}/{self.image_folder}/{file_name}'

    def add_image(self, file_name, title):
        self.report += f"a|image::{self.image_folder}/{file_name}[{title}]\n"

    @classmethod
    def generate_report(cls, loq: CollectResult):
        report = CostReport()

        report.define_version(loq.db_version)
        report.report_config(loq.config, "YB")

        report.report_model(loq.model_queries)

        for query in loq.queries:
            report.add_query(query)

        report.logger.info(f"Queries processed: {len(loq.queries)}")
        report.logger.warn(f"Queries without non-default plans: {report.num_no_opt_queries}")

        report.build_report()
        report.build_xls_report()

        report.publish_report("cost")

    def get_report_name(self):
        return "cost validation"

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    def add_query(self, query: Type[Query]):
        self.logger.debug(f'Processing query ({query.query_hash}): {query.query}...')
        self.add_to_table_row_map(query.tables)

        table_width_map = {}
        for t in query.tables:
            width = 0
            for f in t.fields:
                width += f.avg_width or f.defined_width
            table_width_map[t.alias or t.name] = width

        pf = PlanFeatures()
        ctx = PlanNodeCollectorContext(pf)

        self.process_plans(query.execution_plan, ctx)

        if not query.optimizations:
            self.num_no_opt_queries += 1
        else:
            for plan in query.optimizations:
                if plan.execution_plan and plan.execution_plan.full_str:
                    self.process_plans(plan.execution_plan, ctx)
                    pf.merge(ctx.pf)

        pf.is_single_table = len(query.tables) == 1

        self.logger.debug(f'query features: [{pf}]')

        self.scan_node_map[query.query] = {}

        for table, node_list in chain(ctx.any_index_scan_nodes.items(),
                                      ctx.seq_scan_nodes.items()):
            if table not in self.scan_node_map[query.query]:
                self.scan_node_map[query.query][table] = []
            self.scan_node_map[query.query][table] += node_list

            for node in node_list:
                self.node_projection_width_map[id(node)] = (table_width_map.get(node.table_alias)
                                                            or table_width_map[node.table_name])
                self.logger.debug(
                    '  '.join(
                        filter(lambda prop: prop, [
                            node.name,
                            node.get_index_cond(with_label=True),
                            node.get_remote_tfbr_filter(with_label=True),
                            node.get_remote_filter(with_label=True),
                            node.get_local_filter(with_label=True),
                        ])))
        self.query_map[query.query] = (query, pf)

    def report_queries(self, queries):
        self._start_collapsible("Queries")
        self._start_source(["sql"])
        self.report += "\n".join([query if query.endswith(";") else f"{query};"
                                  for query in sorted(queries)])
        self._end_source()
        self._end_collapsible()

    def report_plot_series(self, plot_series):
        self._start_collapsible("Plot series")
        self._start_source(["text"])
        for series_label, conditions in sorted(plot_series.items()):
            self.report += f"{series_label}\n"
            for cond in sorted(conditions):
                self.report += f"    {cond}\n"
        self._end_source()
        self._end_collapsible()

    def report_plot_series_data(self, plot_series_data, data_labels):
        self._start_collapsible("Plot data")
        self._start_source(["text"])
        if len(plot_series_data):
            self.report += f"{data_labels}\n"
            for series_label, data_points in plot_series_data.items():
                self.report += f"{series_label}\n"
                for tup in zip(data_points.x, data_points.cost, data_points.time_ms):
                    self.report += f"    {tup}\n"
        self._end_source()
        self._end_collapsible()

    def build_report(self):
        self.report += "\n== Scan Nodes\n"

        chart_specs = []

        chart_specs.append(ChartSetSpec(
            'No filter full scans by output rows (width &lt; 2000)',
            'Output rows', 'Estimated cost', 'Execution time [ms]',
            lambda query: True,
            lambda node: (node.has_no_filter() and not node.get_index_cond()
                          and self.get_node_width(node) < 2000),
            lambda node: int(node.rows),
            series_label_suffix = (lambda node: f' {node.index_name or node.table_name} width={self.get_node_width(node)}'),
        ))

        chart_specs.append(ChartSetSpec(
            'No filter full scans by output rows (width &ge; 2000)',
            'Output rows', 'Estimated cost', 'Execution time [ms]',
            lambda query: True,
            lambda node: (node.has_no_filter() and not node.get_index_cond()
                          and self.get_node_width(node) >= 2000),
            lambda node: int(node.rows),
            series_label_suffix = (lambda node: f' {node.index_name or node.table_name} width={self.get_node_width(node)}'),
        ))

        chart_specs.append(ChartSetSpec(
            'No filter full scans by output rows (Index Only Scans, width &lt; 2000)',
            'Output rows', 'Estimated cost', 'Execution time [ms]',
            lambda query: True,
            lambda node: (node.has_no_filter() and not node.get_index_cond()
                          and node.is_index_only_scan and self.get_node_width(node) < 2000),
            lambda node: int(node.rows),
            series_label_suffix = (lambda node: f' width={self.get_node_width(node)}'),
        ))

        chart_specs.append(ChartSetSpec(
            'No filter full scans by output rows (Seq Scans, width &lt; 2000)',
            'Output rows', 'Estimated cost', 'Execution time [ms]',
            lambda query: True,
            lambda node: (node.has_no_filter() and not node.get_index_cond()
                          and node.is_seq_scan and self.get_node_width(node) < 2000),
            lambda node: int(node.rows),
            series_label_suffix = (lambda node: f' {node.table_name} width={self.get_node_width(node)}'),
        ))

        chart_specs.append(ChartSetSpec(
            'No filter index scans and seq scans, simple condition on single key',
            'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
            lambda query: self.nofilter_indexscan_query(query),
            lambda node: (node.table_name != 't1000000m'
                          and (node.is_seq_scan
                               or self.has_simple_index_cond(node, index_cond_only=True))),
            self.get_actual_node_selectivity,
            series_label_suffix = (lambda node: f' {node.index_name or node.table_name}:width={self.get_node_width(node)}'),
        ))

        chart_specs.append(ChartSetSpec(
            'Index scan nodes with literal IN-list',
            'In/out row count ratio', 'Actual-row-count adjusted cost', 'Execution time [ms]',
            lambda query: True,
            lambda node: (self.has_inlist_index_cond(node, parameterized=False)
                          and node.has_no_filter()),
            self.get_actual_node_selectivity,
            series_label_suffix = (lambda node: f'{ node.index_name}:width={self.get_node_width(node)}:items={self.count_inlist_items(node.get_index_cond()[0])}'),
            options=ChartOptions(adjust_cost_by_actual_rows=True)
        ))

        chart_specs.append(ChartSetSpec(
            'Index scan nodes with parameterized IN-list (BNL)',
            'In/out row count ratio', 'Actual-row-count adjusted cost', 'Execution time [ms]',
            lambda query: True,
            lambda node: (self.has_inlist_index_cond(node, parameterized=True)
                          and node.has_no_filter()),
            self.get_actual_node_selectivity,
            series_label_suffix = (lambda node: f'{ node.index_name}:width={self.get_node_width(node)} loops={node.nloops}'),
            options=ChartOptions(adjust_cost_by_actual_rows=True, multipy_by_nloops=True)
        ))

        chart_specs.append(ChartSetSpec(
            'Composite key index scans',
            'In/out row count ratio', 'Actual-row-count adjusted cost', 'Execution time [ms]',
            lambda query: 't1000000m' in query,
            lambda node: (not node.get_local_filter()
                          and ((node.is_seq_scan
                                and (not (expr := node.get_remote_filter())
                                     or self.is_simple_literal_condition(expr)))
                               or self.has_simple_index_cond(node, index_cond_only=True))),
            self.get_actual_node_selectivity,
            series_label_suffix = (lambda node: f'{ node.index_name}'),
            options=ChartOptions(adjust_cost_by_actual_rows=True, multipy_by_nloops=True)
        ))

        chart_specs.append(ChartSetSpec(
            'Index scans with remote index filter and seq scans, simple condition',
            'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
            lambda query: self.scanfilter_indexscan_query(query),
            lambda node: (node.table_name != 't1000000m'
                          and (node.is_seq_scan
                               or ((expr := node.get_remote_filter())
                                   and self.is_simple_literal_condition(expr))
                               or ((expr := node.get_remote_tfbr_filter())
                                   and self.is_simple_literal_condition(expr)))),
            self.get_actual_node_selectivity,
            series_label_suffix = (lambda node: f' {node.index_name or node.table_name}:width={self.get_node_width(node)}'),
        ))

        self.collect_nodes_and_create_plots(chart_specs)

        for spec in chart_specs:
            self.report += f"=== {spec.title}\n"
            self._start_table("3")
            self.report += f"|{spec.xlabel} - {spec.ylabel1}"
            self.report += f"|{spec.xlabel} - {spec.ylabel2}"
            self.report += f"|{spec.ylabel1} - {spec.ylabel2}\n"
            self.add_image(spec.file_names[0], '{spec.ylabel1},align=\"center\"')
            self.add_image(spec.file_names[1], '{spec.ylabel2},align=\"center\"')
            self.add_image(spec.file_names[2], '{spec.ylabel1} - {spec.ylabel2},align=\"center\"')
            self._end_table()

            self._start_table()
            self._start_table_row()
            self.report_queries(spec.queries)
            self.report_plot_series(spec.plot_series)
            self.report_plot_series_data(spec.plot_series_data,
                                         ('{spec.xlabel}', '{spec.ylabel1}', 'spec.ylabel2'))
            self._end_table_row()
            self._end_table()


    def build_xls_report(self):
        pass

    def add_to_table_row_map(self, tables):
        for t in tables:
            self.table_row_map[t.name] = t.rows

    def create_node_plots(self, plot_series_data,
                          title, xvalue_idx, xlabel, yvalue_idx, ylabel,
                          legend=False):
        file_name = self.make_file_name([title, xlabel, ylabel])

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

        if no_data := (len(plot_series_data) == 0):
            plt.text(0.5, 0.5, "NO DATA", size=50, family='sans serif', rotation=30.,
                     ha="center", va="center", alpha=0.4,
                     )

        for (series_label, data_points) in plot_series_data.items():
            plt.plot(data_points[xvalue_idx],
                     data_points[yvalue_idx],
                     data_points.fmt,
                     label=series_label,
                     alpha=0.35)

        if legend:
            plt.legend(fontsize='xx-small',
                       ncols=int((len(plot_series_data.keys())+39)/40.0))

        plt.savefig(self.get_image_path(file_name), dpi=600 if not no_data else 100)
        plt.close()
        return file_name

    def process_plans(self, plan, ctx):
        if not (ptree := plan.parse_plan()):
            self.logger.warn(f"=== Failed to parse plan ===\n{plan.full_str}\n===")
        else:
            if ptree.has_valid_cost():
                PlanNodeCollector(ctx, self.logger).visit(ptree)
            else:
                self.logger.warn(f"=== Skipping plan with invalid costs ===\n{plan.full_str}\n===")

    def nofilter_indexscan_query(self, query_str):
        (q, pf) = self.query_map.get(query_str)
        return pf.has_no_filter_index if pf else False

    def scanfilter_indexscan_query(self, query_str):
        (q, pf) = self.query_map.get(query_str)
        return pf.has_scan_filter_index if pf else False

    def tfbr_filter_indexscan_query(self, query_str):
        (q, pf) = self.query_map.get(query_str)
        return pf.has_tfbr_filter_index if q else False

    def local_filter_query(self, query_str):
        (q, pf) = self.query_map.get(query_str)
        return pf.has_local_filter if q else False

    @staticmethod
    def is_simple_literal_condition(expr):
        if not expr or len(expr) == 0:
            return True
        for branch in re.split(r'AND', expr):
            if re.search(r'[ (]*(c\d+[ )]* *(?:=|>=|<=|<>|<|>) *\d+)', branch):
                pass
            else:
                return False
        return True

    @staticmethod
    def count_inlist_items(expr):
        if ((start := expr.find('= ANY (')) > 0
            and (end := expr.find(')', start)) > 0):
            if m := re.search('\$(?P<first>\d+)[ ,\$0-9]+..., \$(?P<last>\d+)', expr[start:end]):
                first = int(m.group('first'))
                last = int(m.group('last'))
                return last - first + 2, True
            return len(expr[start:end].split(',')), False
        return 0, False

    def has_simple_index_cond(self, node, index_cond_only=False):
        return ((index_cond := str(node.get_index_cond()))
                and self.is_simple_literal_condition(index_cond)
                and (index_cond_only == False
                     or node.has_no_filter()))

    def has_inlist_index_cond(self, node, parameterized=None):
        return ((index_cond := str(node.get_index_cond()))
                and (eq_any_start := index_cond.find('= ANY (')) > 0
                and (eq_any_end := index_cond.find(')', eq_any_start)) > 0
                and (parameterized is None
                     or parameterized == (index_cond.find('$', eq_any_start, eq_any_end) > 0)))

    def get_node_table_rows(self, node):
        return float(self.table_row_map.get(node.table_name))

    def get_node_width(self, node):
        return int(self.node_projection_width_map[id(node)])

    def get_actual_node_selectivity(self, node):
        table_rows = self.get_node_table_rows(node)
        return float(0 if not table_rows or table_rows == 0 else float(node.rows) / table_rows)

    __spcrs = " !\"#$%&'()*+,./:;<=>?[\\]^`{|}~"
    __xtab = str.maketrans(" !\"#$%&'()*+,./:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^`{|}~",
                           "---------------------abcdefghijklmnopqrstuvwxyz---------")
    def make_file_name(self, str_list: list[str]):
        return f"{'-'.join(s.strip(self.__spcrs).translate(self.__xtab) for s in str_list)}.png"

    def collect_nodes_and_create_plots(self, specs: list[ChartSetSpec]):
        self.logger.debug(f'Collecting plot data points...')
        plot_data = []
        for _ in range(len(specs)):
            plot_data.append({})

        for query_str, table_node_list_map in self.scan_node_map.items():
            for table, node_list in table_node_list_map.items():
                for node in node_list:
                    for si, spec in enumerate(specs):
                        if not spec.query_filter(query_str) or not spec.node_filter(node):
                            continue

                        spec.queries.add(query_str)

                        series_label = ''
                        if node.is_seq_scan:
                            series_label += f'Seq Scan'
                        elif node.is_any_index_scan:
                            series_label += ''.join([
                                f"{node.node_type}",
                                ' Backward' if node.is_backward else '',
                            ])
                        else:
                            series_label = node.name

                        series_label += str(spec.series_label_suffix(node)
                                            if spec.series_label_suffix else '')

                        if series_label not in plot_data[si]:
                            plot_data[si][series_label] = []
                            spec.plot_series[series_label] = set()

                        multiplier = (int(node.nloops)
                                      if spec.options.multipy_by_nloops else 1)

                        cost = multiplier * float(node.get_actual_row_adjusted_cost()
                                                  if spec.options.adjust_cost_by_actual_rows
                                                  else node.total_cost)

                        time_ms = float(node.total_ms) * multiplier

                        plot_data[si][series_label].append((float(spec.x_getter(node)),
                                                            cost, time_ms))

                        cond = node.get_search_condition_str(with_label=True)
                        spec.plot_series[series_label].add(f"{cond}" if cond
                                                           else "(No Search Condition)")


        colors = [ 'b', 'g', 'r', 'c', 'm', 'y', 'k' ]
        marker_style = [ 'o', '8', 's', 'p', '*', '+', 'x', 'd',
                    'v', '^', '<', '>', '1', '2', '3', '4',
                    'P', 'h', 'H', 'X', 'D', '|', '_']
        line_style = [ '-', '--', '-.', ':' ]

        rcParams['font.family'] = 'serif'
        rcParams['font.size'] = 6

        for si, spec in enumerate(specs):
            for i, (series_label, node_data) in enumerate(plot_data[si].items()):
                node_data.sort(key=itemgetter(0,2,1)) # xdata,time,cost
                xdata = []
                cost = []
                time_ms = []
                for (x, c, t) in node_data:
                    xdata.append(x)
                    cost.append(c)
                    time_ms.append(t)

                fmt = colors[ i % len(colors) ]
                fmt += marker_style[ i % len(marker_style) ]

                # fmt += line_style[ i % len(line_style) ]
                if re.search('Seq Scan', series_label):
                    fmt += ':'
                elif re.search('Index Scan.*_pkey', series_label):
                    fmt += '-'
                elif re.search('Index Scan', series_label):
                    fmt += '-.'
                elif re.search('Index Only Scan', series_label):
                    fmt += '--'
                else:
                    fmt += ':'

                spec.plot_series_data[series_label] = PlotSeriesData(fmt, xdata, cost, time_ms)

            spec.file_names.append(self.create_node_plots(spec.plot_series_data, spec.title,
                                                          1, spec.xlabel, 2, spec.ylabel1))
            spec.file_names.append(self.create_node_plots(spec.plot_series_data, spec.title,
                                                          1, spec.xlabel, 3, spec.ylabel2,
                                                          legend=True))
            spec.file_names.append(self.create_node_plots(spec.plot_series_data, spec.title,
                                                          2, spec.ylabel1, 3, spec.ylabel2))

            self.logger.debug(f'file_names={spec.file_names}')
