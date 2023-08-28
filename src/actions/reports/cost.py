import html
import inspect
import numpy as np
import re
from collections import namedtuple
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from itertools import chain
from operator import attrgetter

from matplotlib import pyplot as plt
from matplotlib import rcParams

from collect import CollectResult
from objects import PlanNodeVisitor, PlanPrinter, Query
from objects import AggregateNode, JoinNode, SortNode, PlanNode, ScanNode
from actions.report import AbstractReportAction


REPORT_DESCRIPTION = (
    '\n* Each chartset consists of three charts: (x, y) = (cost, exec time), (x-metric, exec time)'
    ' and (x-metric, cost) where `x-metric` is the metric of interest that affects the execution'
    ' time such as node input-output row count ratio, node output data size, etc.'
    '\n\n* The costs are adjusted by the actual row count using the following formula'
    ' unless noted otherwise.'
    '\n\n    (total_cost - startup_cost) * actual_rows / estimated_rows + startup_cost'
    '\n'
)


expr_classifier_pattern = re.compile(
    r'[ (]*((\w+\.)*(?P<column>c\d+)[ )]* *(?P<op>=|>=|<=|<>|<|>)'
    r' *(?P<rhs>(?P<number>\d+)|(?:ANY \(\'{(?P<lit_array>[0-9,]+)}\'::integer\[\]\))'
    r'|(?:ANY \((?P<bnl_array>ARRAY\[[$0-9a-z_,. ]+\])\))))'
)


DataPoint = namedtuple('DataPoint', ['x', 'cost', 'time_ms', 'node'])


@dataclass(frozen=True)
class ChartOptions:
    adjust_cost_by_actual_rows: bool = True
    multipy_by_nloops: bool = False
    log_scale_x: bool = False
    log_scale_cost: bool = False
    log_scale_time: bool = False

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))


@dataclass
class ChartSpec:
    title: str
    description: str
    xlabel: str
    ylabel1: str
    ylabel2: str
    query_filter: Callable[[str], bool]
    node_filter: Callable[[PlanNode], bool]
    x_getter: Callable
    series_label_suffix: Callable = lambda node: ''
    options: ChartOptions = field(default_factory=ChartOptions)

    file_name: str = ''
    queries: set[str] = field(default_factory=set)
    series_data: Mapping[str: Iterable[DataPoint]] = field(default_factory=dict)
    series_format: Mapping[str: str] = field(default_factory=dict)

    def test_node(self, query_str, node):
        return self.query_filter(query_str) and self.node_filter(node)


class NodeFeatures:
    def __init__(self, node: PlanNode):
        self.is_seq_scan = False
        self.is_any_index_scan = False
        self.is_join = False
        self.is_aggregate = False
        self.is_sort = False
        self.has_index_access_cond = False
        self.has_scan_filter = False
        self.has_tfbr_filter = False
        self.has_local_filter = False
        self.has_rows_removed_by_recheck = False

        if isinstance(node, ScanNode):
            self.is_seq_scan = node.is_seq_scan
            self.is_any_index_scan = node.is_any_index_scan
            self.has_index_access_cond = bool(node.get_index_cond())
            self.has_scan_filter = bool(node.get_remote_filter())
            self.has_tfbr_filter = bool(node.get_remote_tfbr_filter())
            self.has_local_filter = bool(node.get_local_filter())
            self.has_rows_removed_by_recheck = bool(node.get_rows_removed_by_recheck())
        elif isinstance(node, JoinNode):
            self.is_join = True
        elif isinstance(node, AggregateNode):
            self.is_aggregate = True
        elif isinstance(node, SortNode):
            self.is_sort = True

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))


@dataclass
class PlanFeatures:
    is_single_table: bool = False
    has_join: bool = False
    has_aggregate: bool = False
    has_sort: bool = False
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

    def update(self, nf: NodeFeatures):
        self.has_join |= nf.is_join
        self.has_aggregate |= nf.is_aggregate
        self.has_sort |= nf.is_sort
        self.has_table_filter_seqscan |= (nf.is_seq_scan and nf.has_scan_filter)
        self.has_local_filter |= nf.has_local_filter
        if nf.is_any_index_scan:
            self.has_key_access_index |= nf.has_index_access_cond
            self.has_scan_filter_index |= nf.has_scan_filter
            self.has_tfbr_filter_index |= nf.has_tfbr_filter
            self.has_no_filter_index |= not (self.has_scan_filter_index
                                             or self.has_tfbr_filter_index
                                             or self.has_local_filter)

    def merge(self, other):
        for a in self.__dict__.keys():
            if getattr(other, a):
                setattr(self, a, True)


@dataclass(frozen=True)
class PlanContext:
    parent_query: Query
    index: int
    plan_tree: PlanNode

    def get_query(self):
        return self.parent_query.optimizations[self.index] if self.index else self.parent_query


@dataclass(frozen=True)
class NodeDetail:
    plan_context: PlanContext
    node_width: int
    node_features: NodeFeatures

    def get_query(self):
        return self.plan_context.get_query()

    def get_plan_tree(self):
        return self.plan_context.plan_tree


class PlanNodeCollectorContext:
    def __init__(self):
        self.seq_scan_nodes: Mapping[str: Iterable[ScanNode]] = dict()
        self.any_index_scan_nodes: Mapping[str: Iterable[ScanNode]] = dict()
        self.pf = PlanFeatures()

    def __str__(self):
        s = ''
        for t, nodes in chain(self.seq_scan_nodes.items(),
                              self.any_index_scan_nodes.items()):
            s += f'  {t}: {len(nodes)} nodes'
            for n in nodes:
                s += f'    {n.get_full_str()}'
        s += f' plan_features: [{self.pf}]'
        return s


class InvalidCostFixer(PlanNodeVisitor):
    def __init__(self, root: PlanNode):
        super().__init__()
        self.root = root
        self.error = False

    def generic_visit(self, node):
        if node.fixup_invalid_cost():
            self.error = True
        else:
            super().generic_visit(node)
        return self.error


class PlanNodeCollector(PlanNodeVisitor):
    def __init__(self, ctx, plan_ctx, node_detail_map, logger):
        super().__init__()
        self.ctx = ctx
        self.plan_ctx = plan_ctx
        self.node_detail_map = node_detail_map
        self.logger = logger
        self.num_scans = 0
        self.depth = 0
        self.scan_node_width_map = self.compute_scan_node_width(plan_ctx.get_query())

    def __enter(self):
        self.ctx.pf.__init__()

    def __exit(self):
        self.ctx.pf.has_single_scan_node = (self.num_scans == 1)
        self.ctx.pf.has_no_condition_scan = not (self.ctx.pf.has_key_access_index
                                                 or self.ctx.pf.has_scan_filter_index
                                                 or self.ctx.pf.has_tfbr_filter_index
                                                 or self.ctx.pf.has_table_filter_seqscan
                                                 or self.ctx.pf.has_local_filter)

    def generic_visit(self, node):
        if self.depth == 0:
            self.__enter()
        self.depth += 1

        node_feat = NodeFeatures(node)
        self.ctx.pf.update(node_feat)
        self.node_detail_map[id(node)] = NodeDetail(self.plan_ctx, None, node_feat)
        super().generic_visit(node)

        self.depth -= 1
        if self.depth == 0:
            self.__exit()

    def visit_scan_node(self, node):
        if self.depth == 0:
            self.__enter()
        self.depth += 1
        self.num_scans += 1

        if int(node.nloops) > 0:
            table = node.table_alias or node.table_name
            node_width = self.scan_node_width_map.get(table)
            # try postgres-generated number suffixed alias
            if (not node_width and node.table_alias
                    and (m := re.fullmatch(fr'({node.table_name})_\d+', node.table_alias))):
                table = m.group(1)
                node_width = self.scan_node_width_map.get(table)
            # use the estimated width if still no avail (TAQO collector was not able to find
            # matching table/field metadata)
            if not node_width:
                node_width = node.plan_width

            node_feat = NodeFeatures(node)
            self.ctx.pf.update(node_feat)
            self.node_detail_map[id(node)] = NodeDetail(self.plan_ctx, node_width, node_feat)

            if node.is_seq_scan:
                if table not in self.ctx.seq_scan_nodes:
                    self.ctx.seq_scan_nodes[table] = []
                self.ctx.seq_scan_nodes[table].append(node)
            elif node.is_any_index_scan:
                if table not in self.ctx.any_index_scan_nodes:
                    self.ctx.any_index_scan_nodes[table] = []
                self.ctx.any_index_scan_nodes[table].append(node)
            else:
                self.logger.warn(f'Unknown ScanNode: node_type={node.node_type}')

        super().generic_visit(node)

        self.depth -= 1
        if self.depth == 0:
            self.__exit()

    @staticmethod
    def compute_scan_node_width(query):
        scan_node_width_map = dict()
        if not query or not query.tables:
            return dict()
        for t in query.tables:
            width = 0
            for f in t.fields:
                width += f.avg_width or f.defined_width
            scan_node_width_map[t.alias or t.name] = width
        return scan_node_width_map


class ExpressionAnalyzer:
    def __init__(self, expr):
        self.expr = expr
        self.columns: set[str] = set()
        self.simple_comp_exprs: int = 0
        self.literal_in_lists: int = 0
        self.bnl_in_lists: int = 0
        self.complex_exprs: int = 0
        self.prop_list: Iterable[Mapping] = list()
        self.__analyze()

    def is_simple_expr(self):
        return (len(self.columns) == 1
                and self.simple_comp_exprs >= 1
                and self.literal_in_lists == 0
                and self.bnl_in_lists == 0
                and self.complex_exprs == 0)

    def __analyze(self):
        if not self.expr or not self.expr.strip():
            return list()
        for branch in re.split('AND', self.expr):
            if m := expr_classifier_pattern.match(branch):
                if column := m.group('column'):
                    self.columns.add(column)
                op = m.group('op')
                rhs = m.group('rhs')
                number = m.group('number')
                self.simple_comp_exprs += bool(column and op and number)

                num_list_items = None

                if literal_array := m.group('lit_array'):
                    num_list_items = len(literal_array.split(','))
                    self.literal_in_lists += 1
                    bnl_array = None
                elif bnl_array := m.group('bnl_array'):
                    self.bnl_in_lists += 1
                    num_list_items = self.__count_inlist_items(bnl_array)

                self.prop_list.append(dict(column=column,
                                           op=op,
                                           rhs=rhs,
                                           number=number,
                                           num_list_items=num_list_items,
                                           literal_array=literal_array,
                                           bnl_array=bnl_array,))
            else:
                self.complex_exprs += 1
                self.prop_list.append(dict(complex=branch))

    @staticmethod
    def __count_inlist_items(expr):
        start = 0
        end = 0
        if ((start := expr.find('= ANY (')) > 0
                and (end := expr.find(')', start)) > 0):
            if m := re.search(r'\$(?P<first>\d+)[ ,\$0-9]+..., \$(?P<last>\d+)',
                              expr[start:end]):
                first = int(m.group('first'))
                last = int(m.group('last'))
                return last - first + 2
            return len(expr[start:end].split(','))
        return 0


class CostReport(AbstractReportAction):
    def __init__(self):
        super().__init__()

        self.interactive = False

        self.report_location = f'report/{self.start_date}'
        self.image_folder = 'imgs'

        self.table_row_map: Mapping[str: float] = dict()
        self.node_detail_map: Mapping[int: NodeDetail] = dict()
        self.scan_node_map: Mapping[str: Mapping[str: Iterable[ScanNode]]] = dict()
        self.query_map: Mapping[str: tuple[Query, PlanFeatures]] = dict()

        self.num_plans: int = 0
        self.num_invalid_cost_plans: int = 0
        self.num_invalid_cost_plans_fixed: int = 0
        self.num_no_opt_queries: int = 0

    def get_image_path(self, file_name):
        return f'{self.report_location}/{self.image_folder}/{file_name}'

    def add_image(self, file_name, title):
        self.report += f"a|image::{self.image_folder}/{file_name}[{title}]\n"

    @classmethod
    def generate_report(cls, loq: CollectResult, interactive):
        report = CostReport()
        report.interactive = interactive

        chart_specs = report.get_chart_specs()

        if interactive:
            chart_specs = report.choose_chart_spec(chart_specs)
        else:
            report.define_version(loq.db_version)
            report.report_config(loq.config, "YB")
            report.report_model(loq.model_queries)

        report.logger.info('Processing queries...')
        for query in sorted(loq.queries, key=lambda query: query.query):
            report.add_query(query)

        report.logger.info(f"Processed {len(loq.queries)} queries  {report.num_plans} plans")
        if report.num_no_opt_queries:
            report.logger.warn(f"Queries without non-default plans: {report.num_no_opt_queries}")
        if report.num_invalid_cost_plans:
            report.logger.warn(f"Plans with invalid costs: {report.num_invalid_cost_plans}"
                               f", fixed: {report.num_invalid_cost_plans_fixed}")

        report.collect_nodes_and_create_plots(chart_specs)

        if not interactive:
            report.build_report(chart_specs)
            report.publish_report("cost")

    def get_report_name(self):
        return "cost validation"

    def define_version(self, version):
        self.report += f"[VERSION]\n====\n{version}\n====\n\n"

    def add_table_row_count(self, tables):
        for t in tables:
            self.table_row_map[t.name] = t.rows

    def process_plan(self, ctx, parent_query, index):
        query = parent_query.optimizations[index] if index else parent_query
        plan = query.execution_plan
        if not (ptree := plan.parse_plan()):
            self.logger.warn(f"=== Failed to parse plan ===\n{plan.full_str}\n")
        else:
            self.num_plans += 1
            if not ptree.has_valid_cost():
                self.num_invalid_cost_plans += 1
                invalid_cost_plan = (f'hints: [{query.explain_hints}]\n'
                                     f'{PlanPrinter.build_plan_tree_str(ptree, actual=False)}')
                self.logger.debug(f'=== Found plan with invalid costs ===\n{invalid_cost_plan}\n')
                if InvalidCostFixer(ptree).visit(ptree):
                    self.logger.warn('*** Failed to fixup invalid costs:\n====\n'
                                     f'{invalid_cost_plan}\n==== Skipping...')
                    return

                self.num_invalid_cost_plans_fixed += 1
                self.logger.debug('=== Fixed up invalid costs successfully ===\n'
                                  f'{PlanPrinter.build_plan_tree_str(ptree, actual=False)}')

        pctx = PlanContext(parent_query, index, ptree)
        PlanNodeCollector(ctx, pctx, self.node_detail_map, self.logger).visit(ptree)

    def add_query(self, query: type[Query]):
        self.logger.debug(f'{query.query_hash}: {query.query}...')
        self.add_table_row_count(query.tables)

        pf = PlanFeatures()

        ctx = PlanNodeCollectorContext()
        self.process_plan(ctx, query, index=None)
        pf.merge(ctx.pf)

        if not query.optimizations:
            self.num_no_opt_queries += 1
        else:
            for ix, opt in enumerate(query.optimizations):
                if opt.execution_plan and opt.execution_plan.full_str:
                    self.process_plan(ctx, query, ix)
                    pf.merge(ctx.pf)

        pf.is_single_table = len(query.tables) == 1

        self.logger.debug(f'query features: [{pf}]')

        self.scan_node_map[query.query] = {}

        for table, node_list in chain(ctx.any_index_scan_nodes.items(),
                                      ctx.seq_scan_nodes.items()):
            if table not in self.scan_node_map[query.query]:
                self.scan_node_map[query.query][table] = []
            self.scan_node_map[query.query][table] += node_list

            # for node in node_list:
            #     self.logger.debug(
            #         '  '.join(
            #             filter(lambda prop: prop, [
            #                 node.name,
            #                 node.get_index_cond(with_label=True),
            #                 node.get_remote_tfbr_filter(with_label=True),
            #                 node.get_remote_filter(with_label=True),
            #                 node.get_local_filter(with_label=True),
            #             ])))
        self.query_map[query.query] = (query, pf)

    def report_chart_filters(self, spec: ChartSpec):
        self._start_collapsible("Chart specifications")
        self._start_source(["python"])
        self.report += "=== Query Filters ===\n"
        self.report += inspect.getsource(spec.query_filter)
        self.report += "=== Node Filters ===\n"
        self.report += inspect.getsource(spec.node_filter)
        self.report += "=== X Axsis Data ===\n"
        self.report += inspect.getsource(spec.x_getter)
        self.report += "=== Series Suffix ===\n"
        self.report += inspect.getsource(spec.series_label_suffix)
        self.report += "=== Options ===\n"
        self.report += str(spec.options)
        self._end_source()
        self._end_collapsible()

    def report_queries(self, queries):
        self._start_collapsible(f"Queries ({len(queries)})")
        self._start_source(["sql"])
        self.report += "\n".join([query if query.endswith(";") else f"{query};"
                                  for query in sorted(queries)])
        self._end_source()
        self._end_collapsible()

    def report_plot_data(self, plot_data, data_labels):
        num_dp = sum([len(cond) for key, cond in plot_data.items()])
        self._start_collapsible(f"Plot data ({num_dp})", sep="=====")
        self.report += "'''\n"
        if plot_data:
            table_header = '|'.join(data_labels)
            table_header += '\n'
            for series_label, data_points in sorted(plot_data.items()):
                self._start_collapsible(f"`{series_label}` ({len(data_points)})")
                self._start_table('3*1m,7a')
                self._start_table_row()
                self.report += table_header
                self._end_table_row()
                for x, cost, time_ms, node in sorted(data_points,
                                                     key=attrgetter('x', 'time_ms', 'cost')):
                    self.report += f"|{x:.3f}\n|{time_ms:.3f}\n|{cost:.3f}\n|\n"
                    self._start_source(["sql"], linenums=False)
                    self.report += str(node)
                    self._end_source()

                self._end_table()
                self._end_collapsible()

        self.report += "'''\n"
        self._end_collapsible(sep="=====")

    def build_report(self, chart_specs):
        self.report += "\n== Description\n"
        self.report += REPORT_DESCRIPTION
        self.report += "\n== Scan Nodes\n"

        for i, spec in enumerate(chart_specs):
            self.report += f"=== {i}. {html.escape(spec.title)}\n"
            self.report += f"{spec.description}\n"
            self._start_table()
            self.add_image(spec.file_name, '{title},align=\"center\"')
            self._end_table()

            self.report_chart_filters(spec)
            self.report_queries(spec.queries)
            self.report_plot_data(spec.series_data, [f'{html.escape(spec.xlabel)}',
                                                     'time_ms', 'cost', 'node'])

    __spcrs = " !\"#$%&'()*+,./:;<=>?[\\]^`{|}~"
    __xtab = str.maketrans(" !\"#$%&'()*+,./:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^`{|}~",
                           "---------------------abcdefghijklmnopqrstuvwxyz---------")

    def make_file_name(self, str_list: Iterable[str]):
        return f"{'-'.join(s.strip(self.__spcrs).translate(self.__xtab) for s in str_list)}.svg"

    def draw_x_cost_time_plot(self, spec):
        title = spec.title
        xy_labels = [spec.xlabel, spec.ylabel1, spec.ylabel2]

        rcParams['font.family'] = 'serif'
        rcParams['font.size'] = 10

        fig, axs = plt.subplots(1, 3, figsize=(27, 8), layout='constrained')
        fig.suptitle(title, fontsize='xx-large')

        chart_ix = [(1, 2), (0, 2), (0, 1)]  # cost-time, x-time, x-cost
        log_scale_axis = [spec.options.log_scale_x,
                          spec.options.log_scale_cost,
                          spec.options.log_scale_time]
        for i in range(len(chart_ix)):
            ax = axs[i]
            x, y = chart_ix[i]
            xlabel = xy_labels[x] + (' (log)' if log_scale_axis[x] else '')
            ylabel = xy_labels[y] + (' (log)' if log_scale_axis[y] else '')

            ax.set_box_aspect(1)
            ax.set_title(f'{xlabel} - {ylabel}', fontsize='x-large')
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            if not spec.series_data:
                ax.text(0.5, 0.5, "NO DATA", size=50, family='sans serif', rotation=30.,
                        ha="center", va="center", alpha=0.4)

        for series_label, data_points in sorted(spec.series_data.items()):
            data_points.sort(key=attrgetter('x', 'time_ms', 'cost'))
            transposed_data = np.split(np.array(data_points).transpose(), len(DataPoint._fields))
            for i in range(len(chart_ix)):
                x, y = chart_ix[i]
                ax = axs[i]
                ax.plot(transposed_data[x][0],
                        transposed_data[y][0],
                        spec.series_format[series_label],
                        label=series_label,
                        alpha=0.35,
                        picker=self.line_picker)

                if log_scale_axis[x]:
                    ax.set_xscale('log')
                    ax.set_xbound(lower=1.0)
                else:
                    ax.set_xbound(lower=0.0)

                if log_scale_axis[y]:
                    ax.set_yscale('log')
                    ax.set_ybound(lower=1.0)
                else:
                    ax.set_ybound(lower=0.0)

        if self.interactive:
            self.show_charts_and_handle_events(spec, fig, axs)
        else:
            if spec.series_data:
                # show the legend on the last subplot
                axs[-1].legend(fontsize='xx-small',
                               ncols=int((len(spec.series_data.keys())+39)/40.0))

            spec.file_name = self.make_file_name([title, xlabel])
            plt.savefig(self.get_image_path(spec.file_name),
                        dpi=50 if spec.series_data else 300)

        plt.close()

    @staticmethod
    def get_series_color(series_label):
        # choices of colors = [ 'b', 'g', 'r', 'c', 'm', 'k' ]
        if 'Seq Scan' in series_label:
            return 'b'
        elif re.search(r'Index Scan.*\(PK\)', series_label):
            return 'm'
        elif 'Index Scan' in series_label:
            return 'r'
        elif 'Index Only Scan' in series_label:
            return 'g'
        return 'k'

    def collect_nodes_and_create_plots(self, specs: Iterable[ChartSpec]):
        self.logger.info('Collecting data points...')

        for query_str, table_node_list_map in self.scan_node_map.items():
            for node_list in table_node_list_map.values():
                for node in node_list:
                    for spec in specs:
                        if not spec.test_node(query_str, node):
                            continue

                        spec.queries.add(query_str)

                        multiplier = (int(node.nloops)
                                      if spec.options.multipy_by_nloops else 1)

                        xdata = round(float(spec.x_getter(node)), 3)
                        cost = round(multiplier * float(node.get_actual_row_adjusted_cost()
                                                        if spec.options.adjust_cost_by_actual_rows
                                                        else node.total_cost), 3)
                        time_ms = round(float(node.total_ms) * multiplier, 3)

                        if node.is_seq_scan:
                            series_label = 'Seq Scan'
                        elif node.is_any_index_scan:
                            series_label = ''.join([
                                f"{node.node_type}",
                                (' (PK)' if '_pkey' in str(node.index_name) else ''),
                                (' Backward' if node.is_backward else '')])
                        else:
                            series_label = node.name

                        if suffix := spec.series_label_suffix(node):
                            series_label += f' {suffix}'

                        if series_label not in spec.series_data:
                            spec.series_data[series_label] = list()

                        spec.series_data[series_label].append(DataPoint(xdata, cost, time_ms, node))

        self.logger.info('Generating plots...')

        marker_style = ['.', 'o', 'v', '^', '<',
                        '>', '8', 's', 'p', '*',
                        'h', 'H', 'D', 'd', 'P', 'X']
        line_style = ['-', '--', '-.', ':']

        for spec in specs:
            for i, (series_label, data_points) in enumerate(sorted(spec.series_data.items())):
                fmt = self.get_series_color(series_label)
                fmt += marker_style[(i+3) % len(marker_style)]
                fmt += line_style[(i+5) % len(line_style)]
                spec.series_format[series_label] = fmt

            self.draw_x_cost_time_plot(spec)

    def choose_chart_spec(self, chart_specs):
        choices = '\n'.join([f'{n}: {s.title}' for n, s in enumerate(chart_specs)])
        while True:
            try:
                response = int(input(f'{choices}\n[0-{len(chart_specs)-1}] --> '))
                if response < 0 or response >= len(chart_specs):
                    raise ValueError
                break
            except ValueError:
                print(f"*** Enter a number in range [0..{len(chart_specs)-1}] ***")
                response = -1
        return [chart_specs[int(response)]]

    @staticmethod
    def line_picker(line, event):
        if event.xdata is None:
            return False, dict()
        ax = event.inaxes
        # convert to display pixel coordinate
        [x], [y] = np.split(ax.transData.transform(line.get_xydata()).T, 2)
        (event_x, event_y) = (event.x, event.y)
        maxd = 10  # pixel radius from the pick event point

        d = np.sqrt((x - event_x)**2 + (y - event_y)**2)
        # print(f'line={line}\n' \
        #       f'x={x}\ny={y}\n' \
        #       f'event_x={event_x} event_y={event_y}\n' \
        #       f'd={d}\n' \
        #       f'ind where (d <= maxd)={np.nonzero(d <= maxd)}')
        ind, = np.nonzero(d <= maxd)
        if len(ind):
            pickx = line.get_xdata()[ind]
            picky = line.get_ydata()[ind]
            [axxy] = ax.transAxes.inverted().transform([(event.x, event.y)])
            props = dict(line=line, ind=ind, pickx=pickx, picky=picky,
                         axx=axxy[0], axy=axxy[1])
            return True, props
        else:
            return False, dict()

    def show_charts_and_handle_events(self, spec, fig, axs):
        def on_pick(event):
            ann = anns[id(event.mouseevent.inaxes)]
            series = event.line.get_label()
            data_point = spec.series_data[series][event.ind[0]]
            node: PlanNode = data_point.node

            modifiers = event.mouseevent.modifiers
            if 'alt' in modifiers:
                ptree = self.get_node_plan_tree(node)
                ann.set_text(f'{PlanPrinter.build_plan_tree_str(ptree)}')
            elif 'shift' in modifiers:
                query = self.get_node_query(node)
                ann.set_text(f'{query.query_hash}\n{query.query}')
            else:
                ann.set_text('\n'.join([
                    series, str(node), node.get_estimate_str(), node.get_actual_str()]))

            ann.xy = event.artist.get_xydata()[event.ind][0]
            ann.xyann = ((event.axx - 0.5)*(-200) - 120, (event.axy - 0.5)*(-200) + 40)

            ann.set_visible(True)
            fig.canvas.draw_idle()

        def on_button_release(event):
            if 'cmd' not in event.modifiers:
                hide_all_annotations()

        def hide_all_annotations():
            redraw = False
            for ann in anns.values():
                redraw |= ann.get_visible()
                ann.set_visible(False)
                if redraw:
                    fig.canvas.draw_idle()

        anns = dict()
        for ax in axs:
            anns[id(ax)] = ax.annotate("", xy=(0, 0),
                                       textcoords="offset points", xytext=(0, 0),
                                       bbox=dict(boxstyle="round", fc="w"),
                                       arrowprops=dict(arrowstyle="->"))
            ann = anns[id(ax)]
            ann.set_wrap(True)
            ann.set_zorder(8)

        hide_all_annotations()
        fig.canvas.mpl_connect('pick_event', on_pick)
        fig.canvas.mpl_connect('button_release_event', on_button_release)
        plt.show()

    def get_node_query(self, node):
        return self.node_detail_map[id(node)].get_query()

    def get_node_query_str(self, node):
        return self.get_node_query(node).query

    def get_node_plan_tree(self, node):
        return self.node_detail_map[id(node)].get_plan_tree()

    def get_node_table_rows(self, node):
        return float(self.table_row_map.get(node.table_name))

    def get_node_width(self, node):
        return (0 if self.is_no_project_query(self.get_node_query(node).query)
                else int(self.node_detail_map[id(node)].node_width))

    def get_actual_node_selectivity(self, node):
        table_rows = float(self.get_node_table_rows(node))
        return (float(node.rows) / table_rows) if table_rows else 0

    def is_scan_with_simple_filter_condition(self, node, allow_local_filter):
        return (isinstance(node, ScanNode)
                and not node.has_no_filter()
                and (allow_local_filter or not node.get_local_filter())
                and self.is_simple_literal_condition(node.get_search_condition_str()))

    def get_plan_features(self, query_str):
        return self.query_map[query_str][1]

    def is_single_table_query(self, query_str):
        return self.get_plan_features(query_str).is_single_table

    def is_no_project_query(self, query_str):
        return (query_str.lower().startswith('select 0 from')
                or query_str.lower().startswith('select count(*) from'))

    def has_no_filter_indexscan(self, query_str):
        return self.get_plan_features(query_str).has_no_filter_index

    def has_scan_filter_indexscan(self, query_str):
        return self.get_plan_features(query_str).has_scan_filter_index

    def has_tfbr_filter_indexscan(self, query_str):
        return self.get_plan_features(query_str).has_tfbr_filter_index

    def has_local_filter(self, query_str):
        return self.get_plan_features(query_str).has_local_filter

    def has_aggregate(self, query_str):
        return self.get_plan_features(query_str).has_aggregate

    @staticmethod
    def is_simple_literal_condition(expr):
        # TODO: cache analyzed results
        return not expr or ExpressionAnalyzer(expr).is_simple_expr()

    @staticmethod
    def count_literal_inlist_items(expr):
        num_item_str_list = list()
        # TODO: cache analyzed results
        for ea_prop in ExpressionAnalyzer(expr).prop_list:
            if ea_prop.get('literal_array'):
                num_item_str_list.append(str(ea_prop.get('num_list_items')))

        return ('x'.join(filter(lambda item: bool(item), sorted(num_item_str_list)))
                if num_item_str_list else '')

    def has_simple_index_cond(self, node, index_cond_only=False):
        index_cond = node.get_index_cond()
        return (index_cond
                and self.is_simple_literal_condition(index_cond)
                and (not index_cond_only
                     or node.has_no_filter()))

    def has_inlist_index_cond(self, node, parameterized=False):
        index_cond = str(node.get_index_cond())
        return (index_cond
                and (eq_any_start := index_cond.find('= ANY (')) > 0
                and (eq_any_end := index_cond.find(')', eq_any_start)) > 0
                and (not parameterized
                     or parameterized == (index_cond.find('$', eq_any_start, eq_any_end) > 0)))

    def get_chart_specs(self):
        return [
            ChartSpec(
                ('Simple index access conditions and corresponding seq scans by node type'
                 ' (t100000 and t100000w)'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: (('t100000 ' in query or 't100000w ' in query)
                               and 'exist' not in query
                               and 'join' not in query
                               and self.has_no_filter_indexscan(query)
                               and not self.has_local_filter(query)
                               and not self.has_aggregate(query)),
                lambda node: (float(self.get_node_table_rows(node)) == 100000
                              and node.get_rows_removed_by_recheck() == 0
                              and (self.has_simple_index_cond(node, index_cond_only=True)
                                   or (node.is_seq_scan and node.get_remote_filter()))),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.table_name}:width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                ('Simple index scans and seq scans, series by index'
                 ' (t100000)'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ('t100000 ' in query
                               and 'exist' not in query
                               and 'join' not in query
                               and self.has_no_filter_indexscan(query)
                               and not self.has_local_filter(query)
                               and not self.has_aggregate(query)),
                lambda node: (float(self.get_node_table_rows(node)) == 100000
                              and node.get_rows_removed_by_recheck() == 0
                              and (self.has_simple_index_cond(node, index_cond_only=True)
                                   or (node.is_seq_scan and node.get_remote_filter()))),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}:'
                                     f'width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                ('Simple index scans and seq scans, series by index'
                 ' (t100000w)'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ('t100000w ' in query
                               and 'exist' not in query
                               and 'join' not in query
                               and self.has_no_filter_indexscan(query)
                               and not self.has_local_filter(query)
                               and not self.has_aggregate(query)),
                lambda node: (float(self.get_node_table_rows(node)) == 100000
                              and node.get_rows_removed_by_recheck() == 0
                              and (self.has_simple_index_cond(node, index_cond_only=True)
                                   or (node.is_seq_scan and node.get_remote_filter()))),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}:'
                                     f'width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                ('Simple index scans and seq scans, series by index'
                 ' (t10000)'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ('t10000 ' in query
                               and 'exist' not in query
                               and 'join' not in query
                               and self.has_no_filter_indexscan(query)
                               and not self.has_local_filter(query)
                               and not self.has_aggregate(query)),
                lambda node: (float(self.get_node_table_rows(node)) == 10000
                              and node.get_rows_removed_by_recheck() == 0
                              and (self.has_simple_index_cond(node, index_cond_only=True)
                                   or (node.is_seq_scan and node.get_remote_filter()))),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}:'
                                     f'width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                ('Simple index scans and seq scans, series by index'
                 ' (t1000)'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ('t1000 ' in query
                               and 'exist' not in query
                               and 'join' not in query
                               and self.has_no_filter_indexscan(query)
                               and not self.has_local_filter(query)
                               and not self.has_aggregate(query)),
                lambda node: (float(self.get_node_table_rows(node)) == 1000
                              and node.get_rows_removed_by_recheck() == 0
                              and (self.has_simple_index_cond(node, index_cond_only=True)
                                   or (node.is_seq_scan and node.get_remote_filter()))),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}:'
                                     f'width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                ('Simple index scans and seq scans, series by index'
                 ' (t100)'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ('t100 ' in query
                               and 'exist' not in query
                               and 'join' not in query
                               and self.has_no_filter_indexscan(query)
                               and not self.has_local_filter(query)
                               and not self.has_aggregate(query)),
                lambda node: (float(self.get_node_table_rows(node)) == 100
                              and node.get_rows_removed_by_recheck() == 0
                              and (self.has_simple_index_cond(node, index_cond_only=True)
                                   or (node.is_seq_scan and node.get_remote_filter()))),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}:'
                                     f'width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                'Index scan nodes with literal IN-list',
                ("Index (Only) Scans with literal IN-list in the index access condition."),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ' in (' in query.lower(),
                lambda node: (self.has_inlist_index_cond(node, parameterized=False)
                              and node.get_rows_removed_by_recheck() == 0
                              and node.has_no_filter()),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name}:width={self.get_node_width(node)}'),
            ),
            ChartSpec(
                'Index scan nodes with literal IN-list (output <= 200 rows)',
                ("Index (Only) Scans with literal IN-list in the index access condition."
                 "\n\n  * The series are grouped by node_width and the number of IN-list items"),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: ' in (' in query.lower(),
                lambda node: (self.has_inlist_index_cond(node, parameterized=False)
                              and node.get_rows_removed_by_recheck() == 0
                              and node.has_no_filter()
                              and float(node.rows) <= 200),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name}'
                                     f':width={self.get_node_width(node)}'
                                     f' IN={self.count_literal_inlist_items(node.get_index_cond())}'),
            ),
            ChartSpec(
                'Parameterized IN-list index scans (BNL)',
                ("Index (Only) Scans with BNL-generaed parameterized IN-list, plus the Seq Scans"
                 " from the same queries."),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (self.has_inlist_index_cond(node, parameterized=True)
                              and node.get_rows_removed_by_recheck() == 0
                              and node.has_no_filter()),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name}:width="'
                                     f'{self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSpec(
                'Composite key index scans',
                ("* The clustered plots near the lower left corner need adjustments the series"
                 " criteria and/or node filtering."
                 "\n\n  * Try adding index key prefix NDV before the first equality to"
                 " the series criteria.\n\ne.g.: for index key `(c3, c4, c5)`,"
                 " condition: `c4 >= x and c5 = y` then the prefix NDV would be:"
                 " `select count(*) from (select distinct c3, c4 from t where c4 >= x) v;`"),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: 't1000000m' in query or 't100000c10' in query,
                lambda node: (self.has_simple_index_cond(node, index_cond_only=True)
                              and node.get_rows_removed_by_recheck() == 0),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=lambda node: f'{node.index_name} loops={node.nloops}',
            ),
            ChartSpec(
                'Composite key index scans (exclude too high costs >= 100000000)',
                ("* The clustered plots near the lower left corner need adjustments the series"
                 " criteria and/or node filtering."
                 "\n\n  * Try adding index key prefix NDV before the first equality to"
                 " the series criteria.\n\ne.g.: for index key `(c3, c4, c5)`,"
                 " condition: `c4 >= x and c5 = y` then the prefix NDV would be:"
                 " `select count(*) from (select distinct c3, c4 from t where c4 >= x) v;`"),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: 't1000000m' in query or 't100000c10' in query,
                lambda node: (self.has_simple_index_cond(node, index_cond_only=True)
                              and node.get_rows_removed_by_recheck() == 0
                              and float(node.total_cost) < 100000000),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=lambda node: f'{node.index_name} loops={node.nloops}',
            ),
            ChartSpec(
                'Composite key index scans (output <= 100 rows)',
                ("* The clustered plots near the lower left corner need adjustments the series"
                 " criteria and/or node filtering."
                 "\n\n  * Try adding index key prefix NDV before the first equality to"
                 " the series criteria.\n\ne.g.: for index key `(c3, c4, c5)`,"
                 " condition: `c4 >= x and c5 = y` then the prefix NDV would be:"
                 " `select count(*) from (select distinct c3, c4 from t where c4 >= x) v;`"),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: 't1000000m' in query or 't100000c10' in query,
                lambda node: (self.has_simple_index_cond(node, index_cond_only=True)
                              and node.get_rows_removed_by_recheck() == 0
                              and float(node.rows) <= 100),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=lambda node: f'{node.index_name} loops={node.nloops}',
            ),
            ChartSpec(
                'Scans with simple remote index and/or table filter',
                "* Index (Only) Scans may or may not have index access condition as well.",
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.has_scan_filter_indexscan(query),
                lambda node: self.is_scan_with_simple_filter_condition(node,
                                                                       allow_local_filter=False),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}'
                                     f':width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSpec(
                'Scans with simple filter(s)',
                'For PG comparisons',
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: self.is_scan_with_simple_filter_condition(node,
                                                                       allow_local_filter=True),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}'
                                     f':width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSpec(
                'Scans with complex (but no IN-lists) remote index and/or table filter',
                "* Index (Only) Scans may or may not have index access condition as well.",
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.has_scan_filter_indexscan(query),
                lambda node: self.is_scan_with_simple_filter_condition(node,
                                                                       allow_local_filter=False),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}'
                                     f':width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSpec(
                'Scans with complex (but no IN-lists) index and/or table filter',
                'For PG comparisons',
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: self.is_scan_with_simple_filter_condition(node,
                                                                       allow_local_filter=True),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}'
                                     f':width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSpec(
                'Full scan + agg push down by table rows (linear scale)',
                ('Scan nodes from `select count(*) from ...` single table queries without'
                 ' any search conditions'
                 '\n\n* The costs are not adjusted'),
                'Table rows', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.has_aggregate(query),
                lambda node: (node.is_scan_with_partial_aggregate()
                              and not node.get_local_filter()),
                x_getter=lambda node: float(self.get_node_table_rows(node)),
                series_label_suffix=lambda node: f'{node.index_name or node.table_name}',
                options=ChartOptions(adjust_cost_by_actual_rows=False),
            ),
            ChartSpec(
                'Full scan + agg push down by table rows (log scale)',
                ('Scan nodes from `select count(*) from ...` single table queries without'
                 ' any search conditions'
                 '\n\n* The costs are not adjusted'),
                'Table rows', 'Estimated cost', 'Execution time [ms]',
                lambda query: (self.has_aggregate(query)
                               and not self.has_local_filter(query)),
                lambda node: node.is_scan_with_partial_aggregate(),
                x_getter=lambda node: float(self.get_node_table_rows(node)),
                series_label_suffix=lambda node: f'{node.index_name or node.table_name}',
                options=ChartOptions(adjust_cost_by_actual_rows=False,
                                     log_scale_x=True,
                                     log_scale_cost=True,
                                     log_scale_time=True),
            ),
            ChartSpec(
                '(EXP) Scans with local filter, may have index access condition and/or remote filter',
                '* need to add the queries and figure out series grouping and query/node selection',
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.has_scan_filter_indexscan(query),
                lambda node: node.get_local_filter(),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node:
                                     f'{node.index_name or node.table_name}'
                                     f':width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSpec(
                '(EXP) No filter full scans by output row x width',
                '* need to adjust series grouping and query/node selection',
                'Output rows x width', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (node.has_no_filter()
                              and not node.get_index_cond()
                              and not node.is_scan_with_partial_aggregate()),
                x_getter=lambda node: float(node.rows) * self.get_node_width(node),
                series_label_suffix=lambda node: f'{node.index_name or node.table_name}',
            ),
            ChartSpec(
                '(EXP) All the scan nodes',
                ('For examining all the collected nodes'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: isinstance(node, ScanNode),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=lambda node: f'width={self.get_node_width(node)}',
            ),
        ]
