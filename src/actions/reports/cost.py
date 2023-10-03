import html
import inspect
import numpy as np
import re
from collections import namedtuple
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from operator import attrgetter

from matplotlib import pyplot as plt
from matplotlib import rcParams

from collect import CollectResult
from objects import PlanPrinter
from objects import PlanNode
from actions.report import AbstractReportAction
from actions.reports.cost_metrics import CostMetrics


BOXPLOT_DESCRIPTION = (
    '=== Boxplot Chart\n'
    '\n```\n'
    '     Q1-1.5IQR   Q1   median  Q3   Q3+1.5IQR\n'
    '     (or min)     |-----:-----|    (or max)\n'
    '  o      |--------|     :     |--------|    o  o\n'
    '                  |-----:-----|\n'
    'flier             <----------->            fliers\n'
    '                       IQR\n'
    '  median: orange solid line\n'
    '  mean: green dashed line\n'
    '```\n'
    '\n* Each box represents the inter-quartile range: [Q1 .. Q3] (+/- 25% from the median).\n'
    '\n* The whiskers extending from the box represent 1.5 x the inter-quantile range.\n'
    '\n* The bubbles beyond the whiskers represent fliers (outliers).\n'
    '\n* References:\n'
    '\n  ** https://en.wikipedia.org/wiki/Box_plot\n'
    '\n  ** http://vita.had.co.nz/papers/boxplots.pdf\n'
    '\n'
)

X_TIME_COST_CHART_DESCRIPTION = (
    '=== Time-Cost Relationship Charts\n'
    '\n* Each chartset consists of three charts: (x, y) = (cost, exec time), (x-metric, exec time)'
    ' and (x-metric, cost) where `x-metric` is the metric of interest that affects the execution'
    ' time such as node input-output row count ratio, node output data size, etc.'
    '\n\n* The costs are adjusted by the actual row count using the following formula'
    ' unless noted otherwise.\n'
    '\n    (total_cost - startup_cost) * actual_rows / estimated_rows + startup_cost\n'
    '\n'
)


DataPoint = namedtuple('DataPoint', ['x', 'cost', 'time_ms', 'node'])


@dataclass(frozen=True)
class ChartOptions:
    adjust_cost_by_actual_rows: bool = True
    multipy_by_nloops: bool = False
    log_scale_x: bool = False
    log_scale_cost: bool = False
    log_scale_time: bool = False

    bp_show_fliers: bool = True  # boxplot only

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))


@dataclass
class ChartSpec:
    plotter: Callable[['ChartSpec'], bool]
    title: str
    description: str
    xlabel: str
    ylabel1: str
    ylabel2: str
    query_filter: Callable[[str], bool]
    node_filter: Callable[[PlanNode], bool]
    x_getter: Callable
    series_suffix: Callable = lambda node: ''
    options: ChartOptions = field(default_factory=ChartOptions)

    xtra_query_filter_list: Iterable[Callable[[str], bool]] = field(default_factory=list)
    xtra_node_filter_list: Iterable[Callable[[PlanNode], bool]] = field(default_factory=list)

    file_name: str = ''
    queries: set[str] = field(default_factory=set)
    series_data: Mapping[str: Iterable[DataPoint]] = field(default_factory=dict)
    series_format: Mapping[str: str] = field(default_factory=dict)
    outliers: Mapping[str: Iterable[DataPoint]] = field(default_factory=dict)
    outlier_axis: str = 'cost/time ratio'

    def is_boxplot(self):
        return self.plotter is CostReport.draw_boxplot

    def make_variant(self, xtra_title, overwrite_title=False,
                     description: str = None,
                     xtra_query_filter: Callable[[str], bool] = None,
                     xtra_node_filter: Callable[[PlanNode], bool] = None,
                     xlabel: str = None,
                     x_getter: Callable = None,
                     series_suffix: str = None,
                     options: ChartOptions = None):
        var = deepcopy(self)
        if overwrite_title:
            var.title = xtra_title
        else:
            var.title += f' ({xtra_title})'
        if description:
            var.description = description
        if xtra_query_filter:
            var.xtra_query_filter_list.append(xtra_query_filter)
        if xtra_node_filter:
            var.xtra_node_filter_list.append(xtra_node_filter)
        if xlabel:
            var.xlabel = xlabel
        if x_getter:
            var.x_getter = x_getter
        if series_suffix:
            var.series_suffix = series_suffix
        if options:
            var.options = options
        return var

    def test_query(self, query_str):
        return all(f(query_str) for f in [self.query_filter, *self.xtra_query_filter_list])

    def test_node(self, node):
        return all(f(node) for f in [self.node_filter, *self.xtra_node_filter_list])


class CostReport(AbstractReportAction):
    def __init__(self):
        super().__init__()
        self.cm = CostMetrics()
        self.interactive = False
        self.report_location = f'report/{self.start_date}'
        self.image_folder = 'imgs'

    def get_image_path(self, file_name):
        return f'{self.report_location}/{self.image_folder}/{file_name}'

    def add_image(self, file_name, title):
        self.content += f"a|image::{self.image_folder}/{file_name}[{title}]\n"

    @classmethod
    def generate_report(cls, loq: CollectResult, interactive):
        report = CostReport()
        cm = report.cm
        report.interactive = interactive

        chart_specs = list()
        if interactive:
            chart_specs = report.choose_chart_spec(report.get_xtc_chart_specs(cm)
                                                   + report.get_exp_chart_specs(cm))
        else:
            report.define_version(loq.db_version)
            report.report_config(loq.config, "YB")
            report.report_model(loq.model_queries)

        report.logger.info('Processing queries...')
        for query in sorted(loq.queries, key=lambda query: query.query):
            cm.add_query(query)

        report.logger.info(f"Processed {len(loq.queries)} queries  {cm.num_plans} plans")
        if cm.num_no_opt_queries:
            report.logger.warn(f"Queries without non-default plans: {cm.num_no_opt_queries}")
        if cm.num_invalid_cost_plans:
            report.logger.warn(f"Plans with invalid costs: {cm.num_invalid_cost_plans}"
                               f", fixed: {cm.num_invalid_cost_plans_fixed}")

        # for now, print and run the queries then populate self.index_prefix_gap_map manually.
        # TODO: move collect and record the ndv to the "collection" step via flag.
        # cm.build_index_prefix_gap_queries()
        # return

        if interactive:
            report.collect_nodes_and_create_plots(chart_specs)
        else:
            dist_chart_specs = report.get_dist_chart_specs(cm)
            xtc_chart_specs = report.get_xtc_chart_specs(cm)
            exp_chart_specs = report.get_exp_chart_specs(cm)
            # exp_chart_specs += report.get_more_exp_chart_specs(cm)
            report.collect_nodes_and_create_plots(dist_chart_specs
                                                  + xtc_chart_specs + exp_chart_specs)
            report.build_report(dist_chart_specs, xtc_chart_specs, exp_chart_specs)
            report.publish_report("cost")

    def get_report_name(self):
        return "cost validation"

    def define_version(self, version):
        self.content += f"[VERSION]\n====\n{version}\n====\n\n"

    def build_report(self, dist_chart_specs, xtc_chart_specs, exp_chart_specs):
        def report_one_chart(self, id, spec):
            self.content  += f"=== {id}. {html.escape(spec.title)}\n{spec.description}\n"
            self.report_chart(spec)

        id = 0
        self.content  += "\n== Time & Cost <<_boxplot_chart, Distribution Charts>>\n"
        for spec in dist_chart_specs:
            report_one_chart(self, id, spec)
            id += 1

        self.content  += "\n== <<_time_cost_relationship_charts, Time - Cost Relationship Charts>>\n"
        for spec in xtc_chart_specs:
            report_one_chart(self, id, spec)
            id += 1

        self.content  += "\n== Experimental Charts\n"
        for spec in exp_chart_specs:
            report_one_chart(self, id, spec)
            id += 1

        self.content  += "== Chart Descriptions\n"
        self.content  += BOXPLOT_DESCRIPTION
        self.content  += X_TIME_COST_CHART_DESCRIPTION

    def report_chart_filters(self, spec: ChartSpec):
        self.start_collapsible("Chart specifications")
        self.start_source(["python"])
        self.content  += "=== Query Filters ===\n"
        for f in spec.query_filter, *spec.xtra_query_filter_list:
            self.content  += inspect.getsource(f)
        self.content  += "=== Node Filters ===\n"
        for f in spec.node_filter, *spec.xtra_node_filter_list:
            self.content  += inspect.getsource(f)
        self.content  += "=== X Axsis Data ===\n"
        self.content  += inspect.getsource(spec.x_getter)
        self.content  += "=== Series Suffix ===\n"
        self.content  += inspect.getsource(spec.series_suffix)
        self.content  += "=== Options ===\n"
        self.content  += str(spec.options)
        self.end_source()
        self.end_collapsible()

    def report_queries(self, queries):
        self.start_collapsible(f"Queries ({len(queries)})")
        self.start_source(["sql"])
        self.content += "\n".join([query if query.endswith(";") else f"{query};"
                                   for query in sorted(queries)])
        self.end_source()
        self.end_collapsible()

    def report_outliers(self, outliers, axis_label, data_labels):
        if not outliers:
            return
        cm = self.cm
        num_dp = sum([len(cond) for key, cond in outliers.items()])
        self.start_collapsible(
            f"#Extreme {axis_label} outliers excluded from the plots ({num_dp})#", sep="=====")
        self.content += "'''\n"
        table_header = '|'.join(data_labels)
        table_header += '\n'
        for series_label, data_points in sorted(outliers.items()):
            self.start_collapsible(f"`{series_label}` ({len(data_points)})")
            self.start_table('<1m,2*^1m,2*5a')
            self.start_table_row()
            self.content += table_header
            self.end_table_row()
            for x, cost, time_ms, node in data_points:
                self.content  += f">|{x:.3f}\n>|{time_ms:.3f}\n>|{cost:.3f}\n|\n"
                self.start_source(["sql"], linenums=False)
                self.content  += str(node)
                self.end_source()
                self.content  += "|\n"
                self.start_source(["sql"], linenums=False)
                self.content  += cm.get_node_query(node).query
                self.end_source()

            self.end_table()
            self.end_collapsible()

        self.content += "'''\n"
        self.end_collapsible(sep="=====")

    def report_plot_data(self, plot_data, data_labels):
        num_dp = sum([len(cond) for key, cond in plot_data.items()])
        self.start_collapsible(f"Plot data ({num_dp})", sep="=====")
        self.content += "'''\n"
        if plot_data:
            table_header = '|'.join(data_labels)
            table_header += '\n'
            for series_label, data_points in sorted(plot_data.items()):
                self.start_collapsible(f"`{series_label}` ({len(data_points)})")
                self.start_table('<1m,2*^1m,8a')
                self.start_table_row()
                self.content += table_header
                self.end_table_row()
                for x, cost, time_ms, node in sorted(data_points,
                                                     key=attrgetter('x', 'time_ms', 'cost')):
                    self.content += f">|{x:.3f}\n>|{time_ms:.3f}\n>|{cost:.3f}\n|\n"
                    self.start_source(["sql"], linenums=False)
                    self.content += str(node)
                    self.end_source()

                self.end_table()
                self.end_collapsible()

        self.content += "'''\n"
        self.end_collapsible(sep="=====")

    def report_stats(self, spec: ChartSpec):
        self.start_table('3,8*^1m')
        self.content += f'|{html.escape(spec.ylabel1)}'
        self.content += '|p0 (min)'
        self.content += '|p25 (Q1)'
        self.content += '|p50{nbsp}(median)'
        self.content += '|mean'
        self.content += '|p75 (Q3)'
        self.content += '|p100 (max)'
        self.content += '|IQR (Q3-Q1)'
        self.content += '|SD\n\n'

        for series_label, data_points in sorted(spec.series_data.items()):
            transposed_data = np.split(np.array(data_points).transpose(), len(DataPoint._fields))
            xdata = transposed_data[0][0]
            ptile = np.percentile(xdata, [0, 25, 50, 75, 100])
            self.content += f'|{series_label}\n'
            self.content += f'>|{ptile[0]:.3f}\n'
            self.content += f'>|{ptile[1]:.3f}\n'
            self.content += f'>|{ptile[2]:.3f}\n'
            self.content += f'>|{np.mean(xdata):.3f}\n'
            self.content += f'>|{ptile[3]:.3f}\n'
            self.content += f'>|{ptile[4]:.3f}\n'
            self.content += f'>|{ptile[3] - ptile[1]:.3f}\n'
            self.content += f'>|{np.std(xdata):.3f}\n'

        self.end_table()

    def report_chart(self, spec: ChartSpec):
        self.start_table()
        self.add_image(spec.file_name, '{title},align=\"center\"')
        self.end_table()
        if spec.is_boxplot():
            self.report_stats(spec)

        self.report_chart_filters(spec)
        self.report_queries(spec.queries)
        self.report_outliers(spec.outliers, spec.outlier_axis,
                             [f'{html.escape(spec.xlabel)}',
                              'time_ms', 'cost', 'node', 'queries'])
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
            cost_per_time = transposed_data[1][0] / transposed_data[2][0]
            if (iqr := np.subtract(*np.percentile(cost_per_time, [75, 25]))) > 0:
                indices = np.nonzero(cost_per_time >
                                     (np.percentile(cost_per_time, [75]) + 4 * iqr))[0]
                outliers = list()
                for ix in reversed(indices):
                    outliers.append(data_points[ix])
                    del data_points[ix]

                if outliers:
                    outliers.sort(key=attrgetter('cost', 'x', 'time_ms'), reverse=True)
                    spec.outliers[series_label] = outliers
                    transposed_data = np.split(np.array(data_points).transpose(),
                                               len(DataPoint._fields))

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
            [self.logger.debug(query_str) for query_str in sorted(spec.queries)]
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

    def draw_boxplot(self, spec):
        title = spec.title
        xlabel = spec.xlabel
        ylabel = spec.ylabel1

        rcParams['font.family'] = 'serif'
        rcParams['font.size'] = 10

        fig, ax = plt.subplots(1, figsize=(12, 2.7), layout='constrained')

        ax.set_title(title, fontsize='large')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        if not spec.series_data:
            ax.text(0.5, 0.5, "NO DATA", size=50, family='sans serif', rotation=30.,
                    ha="center", va="center", alpha=0.4)

        data = list()
        labels = list()
        for series_label, data_points in sorted(spec.series_data.items()):
            transposed_data = np.split(np.array(data_points).transpose(), len(DataPoint._fields))
            xdata = transposed_data[0][0]
            if (iqr := np.subtract(*np.percentile(xdata, [75, 25]))) > 0:
                indices = np.nonzero(xdata > (np.percentile(xdata, [75]) + 3 * iqr))[0]
                outliers = list()
                for ix in reversed(indices):
                    outliers.append(data_points[ix])
                    del data_points[ix]

                if outliers:
                    spec.outlier_axis = "x-axis value"
                    outliers.sort(key=attrgetter('x', 'time_ms', 'cost'), reverse=True)
                    spec.outliers[series_label] = outliers
                    xdata = np.delete(xdata, indices, axis=0)

            data.append(xdata)
            labels.append(series_label)

        ax.boxplot(data, labels=labels, vert=False, meanline=True, showmeans=True,
                   sym=None if spec.options.bp_show_fliers else '')

        ax.xaxis.grid(True)

        if spec.options.log_scale_x:
            ax.set_xscale('log')

        if self.interactive:
            self.show_charts_and_handle_events(spec, fig, [ax])
        else:
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

        for spec in specs:
            for query_str, table_node_list_map in self.cm.query_table_node_map.items():
                if not spec.test_query(query_str):
                    continue
                for node_list in table_node_list_map.values():
                    for node in node_list:
                        if not spec.test_node(node):
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
                                (' (PK)' if node.index_name.endswith('_pkey') else ''),
                                (' Backward' if node.is_backward else ''),
                            ])
                        else:
                            series_label = node.name

                        if suffix := spec.series_suffix(node):
                            series_label += f' {suffix}'

                        spec.series_data.setdefault(series_label, list()).append(
                            DataPoint(xdata, cost, time_ms, node))

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

            spec.plotter(self, spec)

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
                query = self.cm.get_node_query(node)
                ann.set_text(f'{query.query_hash}\n{query.query}')
            else:
                ann.set_text('\n'.join([
                    series,
                    *self.cm.wrap_expr(str(node), 72),
                    node.get_estimate_str(), node.get_actual_str(),
                    f'prefix gaps={self.cm.get_index_key_prefix_gaps(node)}',
                ]))

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

    def get_dist_chart_specs(self, cm: CostMetrics):
        return [
            (boxplot_simple_scan_node := ChartSpec(
                CostReport.draw_boxplot,
                '',
                ('* Nodes with local filter, recheck-removed-rows or partial aggregate'
                 ' are _excluded_\n'),
                'xlabel', 'Node type', '',
                lambda query: True,
                lambda node: (
                    float(node.nloops) == 1
                    and float(node.rows) >= 1
                    and cm.get_node_width(node) == 0
                    and cm.has_no_local_filtering(node)
                    and not cm.has_partial_aggregate(node)
                ),
                x_getter=lambda node: 1,
            )).make_variant(
                'Per row cost/time ratio of the scan nodes (width=0, rows>=1)', True,
                ('  ((total_cost - startup_cost) / estimated_rows)'
                 ' / ((total_time - startup_time) / actual_rows)\n'
                 '\n* Nodes with local filter, recheck-removed-rows or partial aggregate'
                 ' are _excluded_\n'),
                xlabel='Per row cost/time ratio [1/ms]',
                x_getter=lambda node: (cm.get_per_row_cost(node)
                                       / (cm.get_per_row_time(node) or 0.01)),
            ),
            boxplot_simple_scan_node.make_variant(
                'Per row time of the scan nodes (width=0, rows>=1)', True,
                xlabel='Per row execution time[ms]',
                x_getter=lambda node: cm.get_per_row_time(node),
            ),
            boxplot_simple_scan_node.make_variant(
                'Per row cost of the scan nodes (width=0, rows>=1)', True,
                xlabel='Per row cost',
                x_getter=lambda node: cm.get_per_row_cost(node),
            ),
            boxplot_simple_scan_node.make_variant(
                'Startup cost/time ratio of the scan nodes (width=0)', True,
                xlabel='Startup cost/time ratio [1/ms]',
                x_getter=lambda node: (float(node.startup_cost)
                                       / (float(node.startup_ms)
                                          if float(node.startup_ms) else 0.001)),
            ),
            boxplot_simple_scan_node.make_variant(
                'Startup time of the scan nodes (width=0)', True,
                xlabel='Startup time [ms]',
                x_getter=lambda node: float(node.startup_ms),
            ),
        ]

    def get_xtc_chart_specs(self, cm: CostMetrics):
        return (self.get_column_and_value_metric_chart_specs(cm)
                + self.get_simple_index_scan_chart_specs(cm)
                + self.get_in_list_chart_specs(cm))

    def get_exp_chart_specs(self, cm: CostMetrics):
        return self.get_composite_key_access_chart_specs(cm)

    def get_simple_index_scan_chart_specs(self, cm: CostMetrics):
        return [
            (chart_simple_index_scan := ChartSpec(
                CostReport.draw_x_cost_time_plot,
                ('Simple index access conditions and corresponding seq scans by node type'),
                ('Index (Only) Scans with simple index access condition on single key item'
                 ' and the Seq Scans from the same queries.'
                 '\n\n* No IN-list, OR\'ed condition, etc.'
                 '\n\n* The nodes showing "Rows Removed by (Index) Recheck" are excluded.'
                 '\n\n* No nodes from EXISTS and JOIN queries'),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: (
                    cm.is_single_table_query(query)
                    and cm.has_no_filter_indexscan(query)
                    and not cm.has_local_filter(query)
                    and not cm.has_aggregate(query)
                ),
                lambda node: (
                    cm.has_no_local_filtering(node)
                    and not cm.has_no_condition(node)
                    and (node.is_seq_scan
                         or (node.is_any_index_scan
                             and cm.has_only_simple_condition(node, index_cond_only=True)))
                ),
                x_getter=lambda node: float(node.rows),
                series_suffix=(lambda node:
                               f'{node.index_name or node.table_name}:'
                               f'width={cm.get_node_width(node)}'),
            )).make_variant(
                't100000 and t100000w',
                xtra_query_filter=lambda query: 't100000 ' in query or 't100000w ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100000),
                series_suffix=(lambda node:
                               f'{node.table_name}:width={cm.get_node_width(node)}'),
            ),
            chart_simple_index_scan.make_variant(
                't100000',
                xtra_query_filter=lambda query: 't100000 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100000),
            ),
            chart_simple_index_scan.make_variant(
                't100000w',
                xtra_query_filter=lambda query: 't100000w ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100000),
            ),
            chart_simple_index_scan.make_variant(
                't10000',
                xtra_query_filter=lambda query: 't10000 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 10000),
            ),
            chart_simple_index_scan.make_variant(
                't1000',
                xtra_query_filter=lambda query: 't1000 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 1000),
            ),
            chart_simple_index_scan.make_variant(
                't100',
                xtra_query_filter=lambda query: 't100 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100),
            ),
        ]

    def get_column_and_value_metric_chart_specs(self, cm: CostMetrics):
        column_and_value_metric_chart = ChartSpec(
            CostReport.draw_x_cost_time_plot,
            'Column/value num/position metric test queries', '',
            '', 'Estimated cost', 'Execution time [ms]',
            lambda query: (
                cm.is_single_table_query(query)
                and not cm.has_aggregate(query)
            ),
            lambda node: (
                node.table_name == 't1000000c10'
                and not cm.has_partial_aggregate(node)
            ),
            x_getter=lambda node: 0,
        )

        column_and_value_metric_single_column_chart = (
            column_and_value_metric_chart.make_variant(
                'Column/value num/position metric test queries (single column)', '',
                xtra_query_filter=lambda query: (
                    len(cm.get_columns_in_query(query)) == 1
                ),
            )
        )

        return [
            column_and_value_metric_chart.make_variant(
                'Column count (select-list, no condition)', True,
                xtra_node_filter=lambda node: (
                    cm.has_no_condition(node)
                    and (len(cm.get_columns_in_query(cm.get_node_query_str(node))) > 1
                         or (cm.get_columns_in_query(cm.get_node_query_str(node))
                             in (['c4'], ['c5'])))
                ),
                xlabel='Column count',
                x_getter=lambda node: (
                    len(cm.get_columns_in_query(cm.get_node_query_str(node)))
                ),
            ),
            column_and_value_metric_chart.make_variant(
                'Column count (columns in condition)', True,
                xtra_query_filter=lambda query: (
                    re.match(r'select 0 from t1000000c10 where [c0-9+ ]+ = +500000', query)
                ),
                xlabel='Column count',
                x_getter=lambda node: (
                    len(cm.get_columns_in_query(cm.get_node_query_str(node)))
                ),
                series_suffix=lambda node: (
                    ''.join([
                        '(remote filter)' if cm.has_only_scan_filter_condition(node) else ''
                    ])),
            ),
            column_and_value_metric_single_column_chart.make_variant(
                'Column position (select-list, no condition)', True,
                xtra_node_filter=lambda node: (
                    cm.has_no_condition(node)
                    and cm.get_node_width(node) == 4
                ),
                xlabel='Column position',
                x_getter=lambda node: cm.get_single_column_query_column_position(node),
            ),
            column_and_value_metric_single_column_chart.make_variant(
                'Column position (select-list and condition)', True,
                xtra_node_filter=lambda node: (
                    not cm.has_no_condition(node)
                    and cm.has_only_simple_condition(node,
                                                     index_cond_only=False,
                                                     index_key_prefix_only=True)
                    and abs(0.5 - cm.get_single_column_node_normalized_eq_cond_value(node)) < 0.01
                    and cm.get_node_width(node) == 4
                ),
                xlabel='Column position',
                x_getter=lambda node: cm.get_single_column_query_column_position(node),
            ),
            column_and_value_metric_single_column_chart.make_variant(
                'Normalizd value (value position) in condition', True,
                xtra_node_filter=lambda node: (
                    not cm.has_no_condition(node)
                    and cm.has_only_simple_condition(node,
                                                     index_cond_only=True,
                                                     index_key_prefix_only=True)
                    and cm.get_single_column_node_normalized_eq_cond_value(node) is not None
                    and cm.get_node_width(node) == 4
                ),
                xlabel='Normalized value',
                x_getter=lambda node: cm.get_single_column_node_normalized_eq_cond_value(node),
                series_suffix=lambda node: node.index_name or '',
            ),
        ]

    def get_in_list_chart_specs(self, cm: CostMetrics):
        return [
            chart_literal_in_list := ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'Index scan nodes with literal IN-list',
                ("Index (Only) Scans with literal IN-list in the index access condition."),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (
                    cm.has_literal_inlist_index_cond(node)
                    and cm.has_no_local_filtering(node)
                ),
                x_getter=lambda node: float(node.rows),
                series_suffix=(lambda node:
                               ''.join([
                                   f'{node.index_name or node.table_name}:',
                                   f'width={cm.get_node_width(node)}',
                                   ' ncInItems=',
                                   cm.build_non_contiguous_literal_inlist_count_str(
                                       node.table_name, node.get_index_cond()),
                               ])),
            ),
            chart_literal_in_list.make_variant(
                "output <= 200 rows",
                xtra_node_filter=lambda node: float(node.rows) <= 100,
            ),
            ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'Parameterized IN-list index scans (BNL)',
                ("Index (Only) Scans with BNL-generaed parameterized IN-list, plus the Seq Scans"
                 " from the same queries."),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (
                    cm.has_bnl_inlist_index_cond(node)
                    and cm.has_no_local_filtering(node)
                ),
                x_getter=lambda node: float(node.rows),
                series_suffix=(lambda node:
                               f'{node.index_name}:width={cm.get_node_width(node)}'
                               f' loops={node.nloops}'
                               ),
            ),
        ]

    def get_composite_key_access_chart_specs(self, cm: CostMetrics):
        return [
            chart_composite_key := ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'Composite key index scans',
                ("* The clustered plots near the lower left corner need adjustments the series"
                 " criteria and/or node filtering."),
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: 't1000000m' in query or 't1000000c10' in query,
                lambda node: (
                    cm.has_no_local_filtering(node)
                    and cm.has_only_simple_condition(node, index_cond_only=True)
                ),
                x_getter=lambda node: float(node.rows),
                series_suffix=lambda node: f'{node.index_name}',
            ),
            chart_composite_key.make_variant(
                'output rows <= 100',
                xtra_node_filter=lambda node: float(node.rows) <= 100,
            ),
            chart_composite_key.make_variant(
                'output rows <= 100, x=output_row_count x key_prefix_gaps',
                description=(
                    "Index key prefix gaps: NDV of the keys before the first equality condition."
                    "\n\ne.g.: for index key `(c3, c4, c5)`,"
                    " condition: `c4 >= x and c5 = y` then the prefix NDV would be:"
                    " `select count(*) from (select distinct c3, c4 from t where c4 >= x) v;`"),
                xtra_node_filter=lambda node: float(node.rows) <= 100,
                xlabel='Output row count x key prefix gaps',
                x_getter=lambda node: float(node.rows) * cm.get_index_key_prefix_gaps(node),
            ),
            chart_composite_key.make_variant(
                'key_prefix_gaps in series criteria',
                description=(
                    "Index key prefix gaps: NDV of the keys before the first equality condition."
                    "\n\ne.g.: for index key `(c3, c4, c5)`,"
                    " condition: `c4 >= x and c5 = y` then the prefix NDV would be:"
                    " `select count(*) from (select distinct c3, c4 from t where c4 >= x) v;`"),
                series_suffix=lambda node: (f'{node.index_name}'
                                            ' gaps={cm.get_index_key_prefix_gaps(node)}'),
            ),
        ]

    def get_more_exp_chart_specs(self, cm: CostMetrics):
        return [
            ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'Scans with simple remote index and/or table filter',
                "* Index (Only) Scans may or may not have index access condition as well.",
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: cm.has_scan_filter_indexscan(query),
                lambda node: (
                    cm.has_only_scan_filter_condition(node)
                    or cm.has_only_simple_condition(node, index_cond_only=True)
                ),
                x_getter=lambda node: float(node.rows),
                series_suffix=lambda node: (
                    f'{node.index_name or node.table_name}'
                    f':width={cm.get_node_width(node)} loops={node.nloops}'
                ),
            ),
            ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'Scans with remote filter(s)',
                '',
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: cm.has_only_simple_condition(node, index_cond_only=False),
                x_getter=lambda node: float(node.rows),
                series_suffix=(lambda node:
                               f'{node.index_name or node.table_name}'
                               f':width={cm.get_node_width(node)} loops={node.nloops}'),
            ),
            chart_agg_pushdown := ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'Full scan + agg push down by table rows',
                ('Scan nodes from `select count(*) from ...` single table queries without'
                 ' any search conditions'
                 '\n\n* The costs are not adjusted'),
                'Table rows', 'Estimated cost', 'Execution time [ms]',
                lambda query: cm.has_aggregate(query),
                lambda node: (
                    cm.has_partial_aggregate(node)
                    and cm.has_no_local_filtering(node)
                    ),
                x_getter=lambda node: float(cm.get_table_row_count(node.table_name)),
                series_suffix=lambda node: f'{node.index_name or node.table_name}',
                options=ChartOptions(adjust_cost_by_actual_rows=False),
            ),
            chart_agg_pushdown.make_variant(
                'log scale',
                options=ChartOptions(adjust_cost_by_actual_rows=False,
                                     log_scale_x=True,
                                     log_scale_cost=True,
                                     log_scale_time=True),
            ),
            ChartSpec(
                CostReport.draw_x_cost_time_plot,
                'No filter full scans by output row x width',
                '* need to adjust series grouping and query/node selection',
                'Output rows x width', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (node.has_no_filter()
                              and not node.get_index_cond()
                              and not cm.has_partial_aggregate(node)),
                x_getter=lambda node: float(node.rows) * cm.get_node_width(node),
                series_suffix=lambda node: f'{node.index_name or node.table_name}',
            ),
        ]
