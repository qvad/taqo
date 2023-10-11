import re

from collections import namedtuple
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum

from objects import PlanNode
from actions.reports.cost_metrics import CostMetrics


DataPoint = namedtuple('DataPoint', ['x', 'cost', 'time_ms', 'node'])


class PlotType(Enum):
    BOXPLOT = 0
    X_TIME_COST_PLOT = 1


@dataclass(frozen=True)
class ChartOptions:
    adjust_cost_by_actual_rows: bool = False
    multipy_by_nloops: bool = False
    log_scale_x: bool = False
    log_scale_cost: bool = False
    log_scale_time: bool = False

    bp_show_fliers: bool = True  # boxplot only

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))


@dataclass
class ChartSpec:
    plotter: PlotType
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
        return self.plotter is PlotType.BOXPLOT

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


@dataclass
class ChartGroup:
    title: str
    description: str
    chart_specs: Iterable[ChartSpec]


class CostChartSpecs:
    def __init__(self, cm: CostMetrics):
        self.dist_specs = self.__make_dist_specs(cm)
        self.column_and_value_metric_specs = self.__make_column_and_value_metric_specs(cm)
        self.simple_index_scan_specs = self.__make_simple_index_scan_specs(cm)
        self.literal_in_list_specs = self.__make_literal_in_list_specs(cm)
        self.bnl_in_list_specs = self.__make_bnl_in_list_specs(cm)
        self.composite_key_access_specs = self.__make_composite_key_access_specs(cm)
        self.more_exp_specs = self.__make_more_exp_specs(cm)

        self.dist_chart_groups = [
            ChartGroup(
                "Time & cost distribution of scan nodes without any local filtering",
                '',
                self.dist_specs,
            ),
        ]

        self.xtc_chart_groups = [
            ChartGroup(
                "Column/Value Position and Column Count",
                ("1,000,000 row table with all unique columns\n\n"),
                self.column_and_value_metric_specs,
            ),
            ChartGroup(
                "Simple Index Access Conditions",
                'Index scans with simple index access conditions and corresponding seq scans',
                self.simple_index_scan_specs,
            ),
            ChartGroup(
                "Index scan nodes with literal IN-list",
                '',
                self.literal_in_list_specs,
            ),
            ChartGroup(
                "Index scan nodes with parameterized IN-list created by BNL",
                '',
                self.bnl_in_list_specs,
            ),
        ]

        self.exp_chart_groups = [
            ChartGroup(
                "Experimental Charts",
                '',
                self.composite_key_access_specs,
            ),
        ]

        self.more_exp_chart_groups = [
            ChartGroup(
                "More Experimental Charts",
                '',
                self.more_exp_specs,
            ),
        ]

    def get_dist_chart_specs(self):
        return self.dist_specs

    def get_xtc_chart_specs(self):
        return (self.column_and_value_metric_specs
                + self.simple_index_scan_specs
                + self.literal_in_list_specs
                + self.bnl_in_list_specs)

    def get_exp_chart_specs(self):
        return self.composite_key_access_specs

    def get_more_exp_chart_specs(self):
        return self.more_exp_specs

    @staticmethod
    def __make_dist_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        return [
            (boxplot_simple_scan_node := ChartSpec(
                PlotType.BOXPLOT,
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
                options=ChartOptions(adjust_cost_by_actual_rows=True),
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

    @staticmethod
    def __make_column_and_value_metric_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        column_and_value_metric_chart = ChartSpec(
            PlotType.X_TIME_COST_PLOT,
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
            options=ChartOptions(adjust_cost_by_actual_rows=False),
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

    @staticmethod
    def __make_simple_index_scan_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        return [
            (chart_simple_index_scan := ChartSpec(
                PlotType.X_TIME_COST_PLOT,
                ('Index scans with simple index access conditions and corresponding seq scans'),
                '',
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
                options=ChartOptions(adjust_cost_by_actual_rows=True),
            )).make_variant(
                'Table t100000 and t100000w, series by node type', True,
                xtra_query_filter=lambda query: 't100000 ' in query or 't100000w ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100000),
                series_suffix=(lambda node:
                               f'{node.table_name}:width={cm.get_node_width(node)}'),
            ),
            chart_simple_index_scan.make_variant(
                'Table t100000, series by index', True,
                xtra_query_filter=lambda query: 't100000 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100000),
            ),
            chart_simple_index_scan.make_variant(
                'Table t100000w, series by index', True,
                xtra_query_filter=lambda query: 't100000w ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100000),
            ),
            chart_simple_index_scan.make_variant(
                'Table t10000, series by index', True,
                xtra_query_filter=lambda query: 't10000 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 10000),
            ),
            chart_simple_index_scan.make_variant(
                'Table t1000, series by index', True,
                xtra_query_filter=lambda query: 't1000 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 1000),
            ),
            chart_simple_index_scan.make_variant(
                'Table t100, series by index', True,
                xtra_query_filter=lambda query: 't100 ' in query,
                xtra_node_filter=(lambda node:
                                  float(cm.get_table_row_count(node.table_name)) == 100),
            ),
        ]

    @staticmethod
    def __make_literal_in_list_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        return [
            chart_single_literal_in_list := ChartSpec(
                PlotType.X_TIME_COST_PLOT,
                'Index scan nodes with single literal IN-list',
                '',
                'Output row count', 'Estimated cost', 'Execution time [ms]',
                lambda query: True,
                lambda node: (
                    cm.has_literal_inlist_index_cond(node, single_in_list_only=True)
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
            chart_literal_in_list := ChartSpec(
                PlotType.X_TIME_COST_PLOT,
                ('Index scan nodes with literal IN-list'
                 '- 1 or 2 IN-lists, or an IN-list and a simple index access condition'),
                '',
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
            chart_single_literal_in_list.make_variant(
                "output <= 100 rows",
                xtra_node_filter=lambda node: float(node.rows) <= 100,
            ),
            chart_literal_in_list.make_variant(
                "output <= 100 rows",
                xtra_node_filter=lambda node: float(node.rows) <= 100,
            ),
        ]

    @staticmethod
    def __make_bnl_in_list_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        return [
            ChartSpec(
                PlotType.X_TIME_COST_PLOT,
                'Parameterized IN-list index scans (BNL)',
                '',
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
                options=ChartOptions(adjust_cost_by_actual_rows=True),
            ),
        ]

    @staticmethod
    def __make_composite_key_access_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        return [
            chart_composite_key := ChartSpec(
                PlotType.X_TIME_COST_PLOT,
                'Composite key index scans',
                '',
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

    @staticmethod
    def __make_more_exp_specs(cm: CostMetrics) -> Iterable[ChartSpec]:
        return [
            ChartSpec(
                PlotType.X_TIME_COST_PLOT,
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
                PlotType.X_TIME_COST_PLOT,
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
                PlotType.X_TIME_COST_PLOT,
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
                PlotType.X_TIME_COST_PLOT,
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
