import html
import inspect
import numpy as np
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
from objects import Query, PlanNode, ScanNode, PlanNodeVisitor, PlanPrinter
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


PlotSeriesData = namedtuple('PlotSeriesData', ['x', 'cost', 'time_ms', 'node'])


@dataclass
class ChartOptions:
    adjust_cost_by_actual_rows: bool = True
    multipy_by_nloops: bool = False

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))


@dataclass
class ChartSetSpec:
    title: str
    description: str
    xlabel: str
    ylabel1: str
    ylabel2: str
    query_filter: Callable[[str],bool]
    node_filter: Callable[[], bool]
    x_getter: Callable
    series_label_suffix: Callable = None
    options: ChartOptions = field(default_factory=ChartOptions)

    file_name: str = ''
    queries: set[str] = field(default_factory=set)
    series_data: {str: list[PlotSeriesData]} = field(default_factory=dict)
    series_format: {str: str} = field(default_factory=dict)
    series_details: {str: list[str]} = field(default_factory=dict)


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

    def get_query(self):
        return self.plan_context.get_query()

    def get_plan_tree(self):
        return self.plan_context.plan_tree


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

    def visit_plan_node(self, node):
        if self.depth == 0:
            self.__enter()
        self.depth += 1

        self.generic_visit(node)

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
                and (m := re.fullmatch(f'({node.table_name})_\d+', node.table_alias))):
                table = m.group(1)
                node_width = self.scan_node_width_map.get(table)
            # use the estimated width if still no avail (TAQO collector was not able to find
            # matching table/field metadata)
            if not node_width:
                node_width = node.plan_width

            self.node_detail_map[id(node)] = NodeDetail(self.plan_ctx, node_width)

            if node.is_seq_scan:
                if table not in self.ctx.seq_scan_nodes:
                    self.ctx.seq_scan_nodes[table] = []
                self.ctx.seq_scan_nodes[table].append(node)
                self.set_seq_scan_node_features(self.ctx.pf, node)
            elif node.is_any_index_scan:
                if table not in self.ctx.any_index_scan_nodes:
                    self.ctx.any_index_scan_nodes[table] = []
                self.ctx.any_index_scan_nodes[table].append(node)
                self.set_index_scan_node_features(self.ctx.pf, node)
            else:
                self.logger.warn(f'Unknown ScanNode: node_type={node.node_type}')

        self.generic_visit(node)

        self.depth -= 1
        if self.depth == 0:
            self.__exit()

    @staticmethod
    def set_seq_scan_node_features(feat, node):
        feat.has_local_filter = int(node.get_local_filter() is not None)
        feat.has_table_filter_seqscan |= int(node.get_remote_filter() is not None)

    @staticmethod
    def set_index_scan_node_features(feat, node):
        feat.has_local_filter = int(node.get_local_filter()is not None)
        feat.has_key_access_index |= int(node.get_index_cond()is not None)
        feat.has_scan_filter_index |= int(node.get_remote_filter()is not None)
        feat.has_tfbr_filter_index |= int(node.get_remote_tfbr_filter()is not None)
        feat.has_no_filter_index |= not (feat.has_scan_filter_index
                                         or feat.has_tfbr_filter_index
                                         or feat.has_local_filter)

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


class CostReport(AbstractReportAction):
    def __init__(self):
        super().__init__()

        self.interactive = False

        self.report_location = f'report/{self.start_date}'
        self.image_folder = 'imgs'

        self.table_row_map: { str: float } = {}
        self.node_detail_map: { int: NodeDetail } = {}
        self.scan_node_map: { str: { str: [ ScanNode ] } } = {}
        self.query_map: { str: (Query, PlanFeatures) } = {}

        self.num_plans: int = 0
        self.num_invalid_cost_plans: int = 0
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

        for query in loq.queries:
            report.add_query(query)

        report.logger.info(f"Processed {len(loq.queries)} queries  {report.num_plans} plans")
        if report.num_no_opt_queries:
            report.logger.warn(f"Queries without non-default plans: {report.num_no_opt_queries}")
        if report.num_invalid_cost_plans:
            report.logger.warn(f"Plans with invalid costs: {report.num_invalid_cost_plans}")

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

    def process_plans(self, ctx, parent_query, index):
        query = parent_query.optimizations[index] if index else parent_query
        plan = query.execution_plan
        if not (ptree := plan.parse_plan()):
            self.logger.warn(f"=== Failed to parse plan ===\n{plan.full_str}\n===")
        else:
            self.num_plans += 1
            if ptree.has_valid_cost():
                pctx = PlanContext(parent_query, index, ptree)
                PlanNodeCollector(ctx, pctx, self.node_detail_map, self.logger).visit(ptree)
            else:
                self.num_invalid_cost_plans += 1
                self.logger.warn(f"=== Skipping plan with invalid costs ===\n" \
                                 f"hints: [{query.explain_hints}]\n" \
                                 f"{plan.full_str}\n===")

    def add_query(self, query: Type[Query]):
        self.logger.debug(f'Processing query ({query.query_hash}): {query.query}...')
        self.add_table_row_count(query.tables)

        pf = PlanFeatures()
        ctx = PlanNodeCollectorContext(pf)

        self.process_plans(ctx, query, index=None)

        if not query.optimizations:
            self.num_no_opt_queries += 1
        else:
            for ix, opt in enumerate(query.optimizations):
                if opt.execution_plan and opt.execution_plan.full_str:
                    self.process_plans(ctx, query, ix)
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

    def report_chart_filters(self, spec: ChartSetSpec):
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
        self._start_collapsible("Queries")
        self._start_source(["sql"])
        self.report += "\n".join([query if query.endswith(";") else f"{query};"
                                  for query in sorted(queries)])
        self._end_source()
        self._end_collapsible()

    def report_plot_series_details(self, plot_series_details):
        self._start_collapsible("Plot series")
        self._start_source(["text"])
        for series_label, conditions in sorted(plot_series_details.items()):
            self.report += f"{series_label}\n"
            for cond in sorted(conditions):
                self.report += f"    {cond}\n"
        self._end_source()
        self._end_collapsible()

    def report_plot_series_data(self, plot_series_data, data_labels):
        self._start_collapsible("Plot data")
        self._start_source(["text"])
        if plot_series_data:
            self.report += f"{data_labels}\n"
            for series_label, data_points in plot_series_data.items():
                self.report += f"{series_label}\n"
                for x, cost, time_ms, _ in data_points:
                    self.report += f"    ({x}, {cost}, {time_ms})\n"
        self._end_source()
        self._end_collapsible()

    def build_report(self, chart_specs):
        self.report += "\n== Description\n"
        self.report += REPORT_DESCRIPTION
        self.report += "\n== Scan Nodes\n"

        for i, spec in enumerate(chart_specs):
            self.report += f"=== {i}. {html.escape(spec.title)}\n"
            self.report += f"{spec.description}\n"
            self._start_table("1")
            self.add_image(spec.file_name, '{title},align=\"center\"')
            self._end_table()

            self._start_table()
            self._start_table_row()
            self.report_chart_filters(spec)
            self.report_queries(spec.queries)
            self.report_plot_series_details(spec.series_details)
            self.report_plot_series_data(spec.series_data,
                                         (f'{html.escape(spec.xlabel)}',
                                          f'{html.escape(spec.ylabel1)}',
                                          f'{html.escape(spec.ylabel2)}'))
            self._end_table_row()
            self._end_table()

    __spcrs = " !\"#$%&'()*+,./:;<=>?[\\]^`{|}~"
    __xtab = str.maketrans(" !\"#$%&'()*+,./:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^`{|}~",
                           "---------------------abcdefghijklmnopqrstuvwxyz---------")
    def make_file_name(self, str_list: list[str]):
        return f"{'-'.join(s.strip(self.__spcrs).translate(self.__xtab) for s in str_list)}.svg"

    def create_node_plots(self, spec):
        title = spec.title
        xy_labels = [ spec.xlabel, spec.ylabel1, spec.ylabel2 ]

        rcParams['font.family'] = 'serif'
        rcParams['font.size'] = 8

        fig, axs = plt.subplots(1, 3, figsize=(27, 8),
                                layout='constrained')
#                                layout='none' if self.interactive else 'constrained')
        fig.suptitle(title, fontsize='xx-large')

        chart_ix = [(1, 2), (0, 2), (0, 1)] # cost-time, x-time, x-cost
        for i in range(len(chart_ix)):
            ax = axs[i]
            x, y = chart_ix[i]
            xlabel = xy_labels[x]
            ylabel = xy_labels[y]

            ax.set_box_aspect(1)
            ax.set_title(f'{xlabel} - {ylabel}', fontsize='x-large')
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            if not spec.series_data:
                ax.text(0.5, 0.5, "NO DATA", size=50, family='sans serif', rotation=30.,
                        ha="center", va="center", alpha=0.4)

        for series_label, data_points in spec.series_data.items():
            transposed_data = np.split(np.array(data_points).transpose(),
                                       len(PlotSeriesData._fields))
            for i in range(len(chart_ix)):
                x, y = chart_ix[i]
                ax = axs[i]
                ax.plot(transposed_data[x][0],
                        transposed_data[y][0],
                        spec.series_format[series_label],
                        label=series_label,
                        alpha=0.35,
                        picker=self.line_picker)

                ax.set_xbound(lower=0.0)
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
                        dpi=50 if spec.series_data else 600)

        plt.close()

    def get_node_query(self, node):
        return self.node_detail_map[id(node)].get_query()

    def get_node_query_str(self, node):
        return self.get_node_query(node).query

    def get_node_plan_tree(self, node):
        return self.node_detail_map[id(node)].get_plan_tree()

    def get_node_table_rows(self, node):
        return float(self.table_row_map.get(node.table_name))

    def get_node_width(self, node):
        return (0 if self.no_project_query(self.get_node_query(node).query)
                else int(self.node_detail_map[id(node)].node_width))

    def get_actual_node_selectivity(self, node):
        table_rows = self.get_node_table_rows(node)
        return float(0 if not table_rows or table_rows == 0 else float(node.rows) / table_rows)

    def collect_nodes_and_create_plots(self, specs: list[ChartSetSpec]):
        self.logger.debug(f'Collecting plot data points...')

        for query_str, table_node_list_map in self.scan_node_map.items():
            for table, node_list in table_node_list_map.items():
                for node in node_list:
                    for spec in specs:
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

                        multiplier = (int(node.nloops)
                                      if spec.options.multipy_by_nloops else 1)

                        xdata = round(float(spec.x_getter(node)), 3)
                        cost = round(multiplier * float(node.get_actual_row_adjusted_cost()
                                                        if spec.options.adjust_cost_by_actual_rows
                                                        else node.total_cost), 3)
                        time_ms = round(float(node.total_ms) * multiplier, 3)

                        if series_label not in spec.series_data:
                            spec.series_data[series_label] = list()
                            spec.series_details[series_label] = set()

                        query, _ = self.query_map.get(query_str)
                        spec.series_data[series_label].append(
                            PlotSeriesData(xdata, cost, time_ms, node))

                        cond = node.get_search_condition_str(with_label=True)
                        spec.series_details[series_label].add(f"{cond}" if cond
                                                              else "(No Search Condition)")


        colors = [ 'b', 'g', 'r', 'c', 'm', 'y', 'k' ]
        marker_style = [ 'o', '8', 's', 'p', '*', '+', 'x', 'd',
                    'v', '^', '<', '>', '1', '2', '3', '4',
                    'P', 'h', 'H', 'X', 'D', '|', '_']
        line_style = [ '-', '--', '-.', ':' ]

        for spec in specs:
            for i, (series_label, node_data) in enumerate(spec.series_data.items()):
                node_data.sort(key=itemgetter(0,2,1)) # xdata,time,cost

                fmt = colors[ i % len(colors) ]
                fmt += marker_style[ i % len(marker_style) ]

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
                spec.series_format[series_label] = fmt

            self.create_node_plots(spec)

    def choose_chart_spec(self, chart_specs):
        choices = '\n'.join([ f'{n}: {s.title}' for n, s in enumerate(chart_specs) ])
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
        [x], [y] = np.split(ax.transLimits.transform(line.get_xydata()).T, 2)
        event_x, event_y = ax.transLimits.transform((event.xdata, event.ydata))
        maxd = 0.02
        d = np.sqrt((x - event_x)**2 + (y - event_y)**2)
        # print(f'line={line}\n' \
        #       f'x={x}\ny={y}\n' \
        #       f'event_x={event_x} event_y={event_y}\n' \
        #       f'd={d}\n' \
        #       f'np.nonzero(d <= maxd)={np.nonzero(d <= maxd)}')
        ind, = np.nonzero(d <= maxd)

        if len(ind):
            pickx = line.get_xdata()[ind]
            picky = line.get_ydata()[ind]
            props = dict(line=line, ind=ind, pickx=pickx, picky=picky,
                         axx=event_x, axy=event_y)
            return True, props
        else:
            return False, dict()

    def show_charts_and_handle_events(self, spec, fig, axs):
        def on_pick(event):
            ann = anns[id(event.mouseevent.inaxes)]
            series = event.line.get_label()
            series_data = spec.series_data[series][event.ind[0]]
            node: PlanNode = series_data.node

            modifiers = event.mouseevent.modifiers
            if 'alt' in modifiers:
                ptree = self.get_node_plan_tree(node)
                ann.set_text(f'{PlanPrinter.build_plan_tree_str(ptree)}')
            elif 'shift' in modifiers:
                query = self.get_node_query(node)
                ann.set_text(f'{query.query_hash}\n{query.query}')
            else:
                ann.set_text(f'{series}\n{node.name}\n'
                             f'{node.get_estimate_str()}\n{node.get_actual_str()}\n'
                             f'{node.get_search_condition_str(with_label=True)}')

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

    def no_project_query(self, query_str):
        return (query_str.lower().startswith('select 0 from')
                or query_str.lower().startswith('select count(*) from'))

    def no_filter_indexscan_query(self, query_str):
        _, pf = self.query_map.get(query_str)
        return pf.has_no_filter_index if pf else False

    def scan_filter_indexscan_query(self, query_str):
        _, pf = self.query_map.get(query_str)
        return pf.has_scan_filter_index if pf else False

    def tfbr_filter_indexscan_query(self, query_str):
        _, pf = self.query_map.get(query_str)
        return pf.has_tfbr_filter_index if pf else False

    def local_filter_query(self, query_str):
        _, pf = self.query_map.get(query_str)
        return pf.has_local_filter if pf else False

    @staticmethod
    def is_simple_literal_condition(expr):
        if not expr or len(expr) == 0:
            return True
        for branch in re.split(r'AND', expr):
            if re.search(r'[ (]*((\w+\.)*c\d+[ )]* *(?:=|>=|<=|<>|<|>) *\d+)', branch):
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

    def get_chart_specs(self):
        return [
            ChartSetSpec(
                'No filter index scans and seq scans, simple condition on single key',
                ("Index (Only) Scans with simple index access condition on single key item"
                 " and the Seq Scans from the same queries. No IN-list, OR'ed condition, etc."),
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.no_filter_indexscan_query(query),
                lambda node: (self.has_simple_index_cond(node, index_cond_only=True)
                              or (node.is_seq_scan and node.get_remote_filter())),
                self.get_actual_node_selectivity,
                series_label_suffix=(lambda node:
                                     f' {node.index_name or node.table_name}:'
                                     f'width={self.get_node_width(node)}'),
            ),
            ChartSetSpec(
                'Index scan nodes with literal IN-list',
                ("Index (Only) Scans with literal IN-list in the index access condition."
                 "\n\n  * The series are grouped by node_width and the number of IN-list items"),
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (self.has_inlist_index_cond(node, parameterized=False)
                              and node.has_no_filter()),
                self.get_actual_node_selectivity,
                series_label_suffix=(lambda node:
                                     f'{ node.index_name}:width={self.get_node_width(node)}'),
            ),
            ChartSetSpec(
                'Parameterized IN-list index scans (BNL)',
                ("Index (Only) Scans with BNL-generaed parameterized IN-list, plus the Seq Scans"
                 " from the same queries."),
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (self.has_inlist_index_cond(node, parameterized=True)
                              and node.has_no_filter()),
                x_getter=self.get_actual_node_selectivity,
                series_label_suffix=(lambda node: f'{ node.index_name}:width="'
                                     f'{self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSetSpec(
                'Composite key index scans',
                ("* The clustered plots near the lower left corner need adjustments the series"
                 " criteria and/or node filtering."
                 "\n\n  * Try adding index key prefix NDV before the first equality to"
                 " the series criteria.\n\ne.g.: for index key `(c3, c4, c5)`,"
                 " condition: `c4 >= x and c5 = y` then the prefix NDV would be:"
                 " `select count(*) from (select distinct c3, c4 from t where c4 >= x) v;`"),
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: 't1000000m' in query,
                lambda node: (not node.get_local_filter()
                              and ((node.is_seq_scan
                                    and (not (expr := node.get_remote_filter())
                                         or self.is_simple_literal_condition(expr)))
                                   or self.has_simple_index_cond(node, index_cond_only=True))),
                x_getter=self.get_actual_node_selectivity,
                series_label_suffix=(lambda node: f'{ node.index_name} loops={node.nloops}'),
            ),
            ChartSetSpec(
                'Scans with remote index and/or table filter',
                "* Index (Only) Scans may or may not have index access condition as well.",
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.scan_filter_indexscan_query(query),
                lambda node: (node.get_remote_filter()
                              or node.get_remote_tfbr_filter()),
                x_getter=self.get_actual_node_selectivity,
                series_label_suffix=(lambda node: f' {node.index_name or node.table_name}:width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSetSpec(
                'Scans with simple filter(s)',
                "This is to get some data points comparable to 'Scans with remote index and/or table filter' chartset on PG",
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (not node.has_no_filter()
                              and self.is_simple_literal_condition(node.get_search_condition_str())
                              and 'ANY' not in node.get_search_condition_str()),
                x_getter=self.get_actual_node_selectivity,
                series_label_suffix=(lambda node: f' {node.index_name or node.table_name}:width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSetSpec(
                '(WIP) Scans with local filter, may have index access condition and/or remote filter',
                '* need to add the queries and figure out series grouping and query/node selection',
                'In/out row count ratio', 'Estimated cost', 'Execution time [ms]',
                lambda query: self.scan_filter_indexscan_query(query),
                lambda node: node.get_local_filter(),
                x_getter=self.get_actual_node_selectivity,
                series_label_suffix=(lambda node: f' {node.index_name or node.table_name}:width={self.get_node_width(node)} loops={node.nloops}'),
            ),
            ChartSetSpec(
                '(WIP) No filter full scans by output rows',
                '* need to adjust series grouping and query/node selection',
                'Output rows', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (node.has_no_filter() and not node.get_index_cond()),
                x_getter=lambda node: float(node.rows),
                series_label_suffix=(lambda node: f' {node.index_name or node.table_name} width={self.get_node_width(node)}'),
            ),
        ]
