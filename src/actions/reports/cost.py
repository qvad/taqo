import html
import inspect
import numpy as np
import re
from collections.abc import Iterable
from operator import attrgetter

from matplotlib import pyplot as plt
from matplotlib import rcParams

from collect import CollectResult
from objects import PlanPrinter
from objects import PlanNode
from actions.report import AbstractReportAction, SubReport
from actions.reports.cost_metrics import CostMetrics
from actions.reports.cost_chart_specs import (CostChartSpecs, ChartGroup, ChartSpec,
                                              DataPoint, PlotType)


IMAGE_FILE_SUFFIX = '.svg'


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
    '=== X-Time-Cost Relationship Charts\n'
    '\n* Each chartset consists of three charts: (x, y) = (cost, exec time), (x-metric, exec time)'
    ' and (x-metric, cost) where `x-metric` is the metric of interest that affects the execution'
    ' time such as node input-output row count ratio, node output data size, etc.'
    '\n\n* The costs are adjusted by the actual row count using the following formula'
    ' unless noted otherwise.\n'
    '\n    (total_cost - startup_cost) * actual_rows / estimated_rows + startup_cost\n'
    '\n'
)


class CostReport(AbstractReportAction):
    def __init__(self):
        super().__init__()
        self.cm = CostMetrics()
        self.cs = CostChartSpecs(self.cm)
        self.interactive = False
        self.report_location = f'report/{self.start_date}'
        self.image_folder = 'imgs'

    def get_image_location(self):
        return f'{self.report_location}/{self.image_folder}/'

    def make_image_block(self, file_name, attrs, is_sub_report=False):
        pre = 'image::'
        if is_sub_report:
            pre += '../'
        return f'{pre}{self.image_folder}/{file_name}[{attrs}]\n'

    @classmethod
    def generate_report(cls, loq: CollectResult, interactive):
        report = CostReport()
        cm = report.cm
        cs = report.cs
        report.interactive = interactive

        chart_specs = list()
        if interactive:
            chart_specs = report.choose_chart_spec(cs.get_xtc_chart_specs()
                                                   + cs.get_exp_chart_specs())
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
            report.collect_nodes_and_create_plots(
                cs.get_dist_chart_specs()
                + cs.get_xtc_chart_specs()
                + cs.get_exp_chart_specs()
                + cs.get_more_exp_chart_specs()
            )

            report.build_report()
            report.publish_report("cost")

    def get_report_name(self):
        return "cost validation"

    def define_version(self, version):
        self.content += f"[VERSION]\n====\n{version}\n====\n\n"

    def build_report(self):
        id = 0

        self.content += "\n== Time & Cost Distribution Charts\n"
        self.content += "\n<<_boxplot_chart, Boxplot distribution chart description>>\n"
        self.report_chart_groups(id, self.cs.dist_chart_groups)

        self.content += "\n== Time - Cost Relationship Charts\n"
        self.content += "\n<<_x_time_cost_relationship_charts, time - cost chart description>>\n"
        id += self.report_chart_groups(id, self.cs.xtc_chart_groups)

        id += self.report_chart_groups(id, self.cs.exp_chart_groups)
        id += self.report_chart_groups(id, self.cs.more_exp_chart_groups)

        self.content += "== Chart Descriptions\n"
        self.content += BOXPLOT_DESCRIPTION
        self.content += X_TIME_COST_CHART_DESCRIPTION

    def report_chart_groups(self, start_id: int, chart_groups: Iterable[ChartGroup]):
        id = start_id
        cols = 3
        i = 0
        for cg in chart_groups:
            self.content += f"\n=== {cg.title}\n"
            self.content += f"\n{cg.description}\n"
            self.start_table(cols)
            title_row = ''
            image_row = ''
            for spec in filter(lambda s: bool(s.file_name), cg.chart_specs):
                sub_report_tag = spec.file_name.replace(IMAGE_FILE_SUFFIX, '')
                title = html.escape(f'{id} {spec.title}')
                sub_report = self.create_sub_report(sub_report_tag)
                sub_report.content += f"\n[#{sub_report_tag}]\n"
                sub_report.content += f"== {title}\n\n{spec.description}\n\n"
                self.report_chart(sub_report, spec)
                dpstr = f'{sum([len(dp) for dp in spec.series_data.values()])} data points'
                olstr = (f' after excluding #{sum([len(dp) for dp in spec.outliers.values()])}'
                         ' extreme outliers#') if spec.outliers else ''
                title_row += f'|{title} +\n({dpstr}{olstr})'

                image_row += 'a|'
                image_attrs = (f'link="tags/{sub_report_tag}.html",align="center"')
                image_row += self.make_image_block(spec.file_name, image_attrs)
                if i % cols == 2:
                    self.content += title_row
                    self.content += '\n\n'
                    self.content += image_row
                    title_row = ''
                    image_row = ''
                i += 1
                id += 1
            while i % cols != 0:
                title_row += '|'
                image_row += 'a|\n'
                i += 1
            self.content += title_row
            self.content += '\n'
            self.content += image_row

            self.end_table()
        return id - start_id

    @staticmethod
    def report_chart_filters(report: SubReport, spec: ChartSpec):
        report.start_collapsible("Chart specifications")
        report.start_source(["python"])
        report.content += "=== Query Filters ===\n"
        for f in spec.query_filter, *spec.xtra_query_filter_list:
            report.content += inspect.getsource(f)
        report.content += "=== Node Filters ===\n"
        for f in spec.node_filter, *spec.xtra_node_filter_list:
            report.content += inspect.getsource(f)
        report.content += "=== X Axsis Data ===\n"
        report.content += inspect.getsource(spec.x_getter)
        report.content += "=== Series Suffix ===\n"
        report.content += inspect.getsource(spec.series_suffix)
        report.content += "=== Options ===\n"
        report.content += str(spec.options)
        report.end_source()
        report.end_collapsible()

    @staticmethod
    def report_queries(report: SubReport, queries):
        report.start_collapsible(f"Queries ({len(queries)})")
        report.start_source(["sql"])
        report.content += "\n".join([query if query.endswith(";") else f"{query};"
                                     for query in sorted(queries)])
        report.end_source()
        report.end_collapsible()

    @staticmethod
    def report_outliers(report: SubReport, cm: CostMetrics, outliers, axis_label, data_labels):
        if not outliers:
            return
        num_dp = sum([len(dp) for dp in outliers.values()])
        report.start_collapsible(
            f"#Extreme {axis_label} outliers excluded from the plots ({num_dp})#", sep="=====")
        report.content += "'''\n"
        table_header = '|'.join(data_labels)
        table_header += '\n'
        for series_label, data_points in sorted(outliers.items()):
            report.start_collapsible(f"`{series_label}` ({len(data_points)})")
            report.start_table('<1m,2*^1m,2*5a')
            report.start_table_row()
            report.content += table_header
            report.end_table_row()
            for x, cost, time_ms, node in data_points:
                report.content += f">|{x:.3f}\n>|{time_ms:.3f}\n>|{cost:.3f}\n|\n"
                report.start_source(["sql"], linenums=False)
                report.content += str(node)
                report.end_source()
                report.content += "|\n"
                report.start_source(["sql"], linenums=False)
                report.content += cm.get_node_query(node).query
                report.end_source()

            report.end_table()
            report.end_collapsible()

        report.content += "'''\n"
        report.end_collapsible(sep="=====")

    @staticmethod
    def report_plot_data(report: SubReport, plot_data, data_labels):
        num_dp = sum([len(dp) for key, dp in plot_data.items()])
        report.start_collapsible(f"Plot data ({num_dp})", sep="=====")
        report.content += "'''\n"
        if plot_data:
            table_header = '|'.join(data_labels)
            table_header += '\n'
            for series_label, data_points in sorted(plot_data.items()):
                report.start_collapsible(f"`{series_label}` ({len(data_points)})")
                report.start_table('<1m,2*^1m,8a')
                report.start_table_row()
                report.content += table_header
                report.end_table_row()
                for x, cost, time_ms, node in sorted(data_points,
                                                     key=attrgetter('x', 'time_ms', 'cost')):
                    report.content += f">|{x:.3f}\n>|{time_ms:.3f}\n>|{cost:.3f}\n|\n"
                    report.start_source(["sql"], linenums=False)
                    report.content += str(node)
                    report.end_source()

                report.end_table()
                report.end_collapsible()

        report.content += "'''\n"
        report.end_collapsible(sep="=====")

    @staticmethod
    def report_stats(report: SubReport, spec: ChartSpec):
        report.start_table('3,8*^1m')
        report.content += f'|{html.escape(spec.ylabel1)}'
        report.content += '|p0 (min)'
        report.content += '|p25 (Q1)'
        report.content += '|p50{nbsp}(median)'
        report.content += '|mean'
        report.content += '|p75 (Q3)'
        report.content += '|p100 (max)'
        report.content += '|IQR (Q3-Q1)'
        report.content += '|SD\n\n'

        for series_label, data_points in sorted(spec.series_data.items()):
            transposed_data = np.split(np.array(data_points).transpose(), len(DataPoint._fields))
            xdata = transposed_data[0][0]
            ptile = np.percentile(xdata, [0, 25, 50, 75, 100])
            report.content += f'|{series_label}\n'
            report.content += f'>|{ptile[0]:.3f}\n'
            report.content += f'>|{ptile[1]:.3f}\n'
            report.content += f'>|{ptile[2]:.3f}\n'
            report.content += f'>|{np.mean(xdata):.3f}\n'
            report.content += f'>|{ptile[3]:.3f}\n'
            report.content += f'>|{ptile[4]:.3f}\n'
            report.content += f'>|{ptile[3] - ptile[1]:.3f}\n'
            report.content += f'>|{np.std(xdata):.3f}\n'

        report.end_table()

    def report_chart(self, report: SubReport, spec: ChartSpec):
        report.start_table()
        report.content += 'a|'
        report.content += self.make_image_block(spec.file_name, f'{spec.title},align="center"',
                                                True)
        report.end_table()
        if spec.is_boxplot():
            CostReport.report_stats(report, spec)

        CostReport.report_chart_filters(report, spec)
        CostReport.report_queries(report, spec.queries)
        CostReport.report_outliers(report, self.cm,
                                   spec.outliers, spec.outlier_axis,
                                   [f'{html.escape(spec.xlabel)}',
                                    'time_ms', 'cost', 'node', 'queries'])
        CostReport.report_plot_data(report, spec.series_data, [f'{html.escape(spec.xlabel)}',
                                                               'time_ms', 'cost', 'node'])

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

        plotters = {
            PlotType.BOXPLOT: CostReport.draw_boxplot,
            PlotType.X_TIME_COST_PLOT: CostReport.draw_x_time_cost_plot,
        }

        for spec in specs:
            for i, (series_label, data_points) in enumerate(sorted(spec.series_data.items())):
                fmt = self.get_series_color(series_label)
                fmt += marker_style[(i+3) % len(marker_style)]
                fmt += line_style[(i+5) % len(line_style)]
                spec.series_format[series_label] = fmt

            plotters[spec.plotter](self, spec)

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

    __xtab = str.maketrans(" !\"#$%&'()*+,./:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^`{|}~",
                           "---------------------abcdefghijklmnopqrstuvwxyz---------")
    __fno = -1

    @staticmethod
    def make_file_name(name: str):
        CostReport.__fno += 1
        return (f"{CostReport.__fno:06d}-"
                f"{re.sub(r'-+', '-', name.translate(CostReport.__xtab).strip('-'))}"
                f"{IMAGE_FILE_SUFFIX}")

    def draw_x_time_cost_plot(self, spec):
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

            spec.file_name = self.make_file_name('-'.join([title, xlabel]))
            plt.savefig(self.get_image_location() + spec.file_name,
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
            spec.file_name = self.make_file_name('-'.join([title, xlabel]))
            plt.savefig(self.get_image_location() + spec.file_name,
                        dpi=50 if spec.series_data else 300)

        plt.close()

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
                ptree = self.cm.get_node_plan_tree(node)
                ann.set_text(PlanPrinter.build_plan_tree_str(ptree))
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
