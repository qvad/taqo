import re

from collections.abc import Iterable, Mapping
from config import Config
from dataclasses import dataclass
from itertools import pairwise

from objects import PlanNodeVisitor, PlanPrinter, Query
from objects import AggregateNode, JoinNode, SortNode, PlanNode, ScanNode

from actions.reports.cost_metric_metadata import column_stats_map, index_prefix_gap_map


expr_classifier_pattern = re.compile(
    r'[ (]*((\w+\.)*(?P<column>c\d+)[ )]* *(?P<op>=|>=|<=|<>|<|>)'
    r' *(?P<rhs>(?P<number>\d+)|(?:ANY \(\'{(?P<lit_array>[0-9,]+)}\'::integer\[\]\))'
    r'|(?:ANY \((?P<bnl_array>ARRAY\[[$0-9a-z_,. ]+\])\))))'
)

in_list_item_extraction_pattern = re.compile(
    r'\$(?P<first>\d+)[ ,\$0-9]+..., \$(?P<last>\d+)'
)

# assume cost-validation model naming convention for now:
#   <index name> : <table name>_<key columns>_<included columns>
#   <column> : [cv]\d*
#   <key columns> | <included columns> : <column>{<column>...}
index_key_extraction_pattern = re.compile(
    r'(?P<table>\w[0-9a-z]+)_(?P<key>\w[0-9a-z]+)(?:_(?P<inc>\w[0-9a-z]+))*'
)
packed_column_list_pattern = re.compile(r'[cv]\d*')


class NodeClassifiers:
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
        self.has_no_condition = False
        self.has_partial_aggregate = False

        if isinstance(node, ScanNode):
            self.is_seq_scan = node.is_seq_scan
            self.is_any_index_scan = node.is_any_index_scan
            self.has_index_access_cond = bool(node.get_index_cond())
            self.has_scan_filter = bool(node.get_remote_filter())
            self.has_tfbr_filter = bool(node.get_remote_tfbr_filter())
            self.has_local_filter = bool(node.get_local_filter())
            self.has_rows_removed_by_recheck = bool(node.get_rows_removed_by_recheck())
            self.has_partial_aggregate = node.is_scan_with_partial_aggregate()
            self.has_no_condition = not any([
                self.has_index_access_cond,
                self.has_scan_filter,
                self.has_tfbr_filter,
                self.has_local_filter,
            ])
        elif isinstance(node, JoinNode):
            self.is_join = True
        elif isinstance(node, AggregateNode):
            self.is_aggregate = True
        elif isinstance(node, SortNode):
            self.is_sort = True

    def __str__(self):
        return ','.join(filter(lambda a: getattr(self, a), self.__dict__.keys()))


@dataclass
class PlanClassifiers:
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

    def update(self, nc: NodeClassifiers):
        self.has_join |= nc.is_join
        self.has_aggregate |= nc.is_aggregate
        self.has_sort |= nc.is_sort
        self.has_table_filter_seqscan |= (nc.is_seq_scan and nc.has_scan_filter)
        self.has_local_filter |= nc.has_local_filter
        if nc.is_any_index_scan:
            self.has_key_access_index |= nc.has_index_access_cond
            self.has_scan_filter_index |= nc.has_scan_filter
            self.has_tfbr_filter_index |= nc.has_tfbr_filter
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
class NodeContext:
    plan_context: PlanContext
    node_width: int
    node_classifiers: NodeClassifiers

    def get_parent_query(self):
        return self.plan_context.parent_query

    def get_query(self):
        return self.plan_context.get_query()

    def get_plan_tree(self):
        return self.plan_context.plan_tree


@dataclass(frozen=True)
class QueryContext:
    query: Query
    pc: PlanClassifiers

    def get_columns(self):
        return sorted(set(f.name for t in self.query.tables for f in t.fields))


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

    def has_key_prefix_cond_only(self, key_cols):
        cols = set(self.columns)  # make a copy
        for kc in key_cols:
            if kc in cols:
                cols.remove(kc)
                if not len(cols):
                    return True
        return False

    def __analyze(self):
        if not self.expr or not self.expr.strip():
            return list()
        for branch in re.split(r'\bAND\b', self.expr):
            if m := expr_classifier_pattern.search(branch):
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
            if m := in_list_item_extraction_pattern.search(expr[start:end]):
                first = int(m.group('first'))
                last = int(m.group('last'))
                return last - first + 2
            return len(expr[start:end].split(','))
        return 0


class PlanNodeCollectorContext:
    def __init__(self):
        self.table_node_map: Mapping[str: Iterable[ScanNode]] = dict()
        self.pc = PlanClassifiers()

    def __str__(self):
        s = ''
        for t, nodes in self.scan_nodes.items():
            s += f'  {t}: {len(nodes)} nodes'
            for n in nodes:
                s += f'    {n.get_full_str()}'
        s += f' plan_classifiers: [{self.pc}]'
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
    def __init__(self, ctx, plan_ctx, node_context_map, logger):
        super().__init__()
        self.ctx = ctx
        self.plan_ctx = plan_ctx
        self.node_context_map = node_context_map
        self.logger = logger
        self.num_scans = 0
        self.depth = 0
        self.scan_node_width_map = self.compute_scan_node_width(plan_ctx.get_query())

    def __enter(self):
        self.ctx.pc.__init__()

    def __exit(self):
        self.ctx.pc.has_single_scan_node = (self.num_scans == 1)
        self.ctx.pc.has_no_condition_scan = not any([
            self.ctx.pc.has_key_access_index,
            self.ctx.pc.has_scan_filter_index,
            self.ctx.pc.has_tfbr_filter_index,
            self.ctx.pc.has_table_filter_seqscan,
            self.ctx.pc.has_local_filter,
        ])

    def generic_visit(self, node):
        if self.depth == 0:
            self.__enter()
        self.depth += 1

        classifiers = NodeClassifiers(node)
        self.ctx.pc.update(classifiers)
        self.node_context_map[id(node)] = NodeContext(self.plan_ctx, None, classifiers)
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

            classifiers = NodeClassifiers(node)
            self.ctx.pc.update(classifiers)
            self.node_context_map[id(node)] = NodeContext(self.plan_ctx, node_width, classifiers)

            if (node.is_seq_scan
                    or node.is_any_index_scan
                    or node.node_type in ['Bitmap Index Scan', 'Bitmap Heap Scan']):
                self.ctx.table_node_map.setdefault(table, list()).append(node)
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


class CostMetrics:
    def __init__(self):
        self.logger = Config().logger
        self.table_row_map: Mapping[str: float] = dict()
        self.column_position_map: Mapping[str: int] = dict()
        self.node_context_map: Mapping[int: NodeContext] = dict()
        self.query_context_map: Mapping[str: QueryContext] = dict()
        self.query_table_node_map: Mapping[str: Mapping[str: Iterable[ScanNode]]] = dict()
        self.expr_analyzers: Mapping[str: ExpressionAnalyzer] = dict()

        self.num_plans: int = 0
        self.num_invalid_cost_plans: int = 0
        self.num_invalid_cost_plans_fixed: int = 0
        self.num_no_opt_queries: int = 0

    def add_table_metadata(self, tables):
        for t in tables:
            self.table_row_map[t.name] = t.rows
            for f in t.fields:
                self.column_position_map[f'{t.name}:{f.name}'] = f.position

    def process_plan(self, ctx, parent_query, index):
        query = parent_query.optimizations[index] if index else parent_query
        if not (plan := query.execution_plan):
            self.logger.warn(f"=== Query ({index or 'default'}): [{query.query}]"
                             " does not have any valid plan\n")
            return

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
        PlanNodeCollector(ctx, pctx, self.node_context_map, self.logger).visit(ptree)

    def add_query(self, query: type[Query]):
        self.logger.debug(f'Adding {query.tag} {query.query_hash}: {query.query}...')
        self.add_table_metadata(query.tables)

        pc = PlanClassifiers()

        ctx = PlanNodeCollectorContext()
        self.process_plan(ctx, query, index=None)
        pc.merge(ctx.pc)

        if not query.optimizations:
            self.num_no_opt_queries += 1
        else:
            for ix, opt in enumerate(query.optimizations):
                if opt.execution_plan and opt.execution_plan.full_str:
                    self.process_plan(ctx, query, ix)
                    pc.merge(ctx.pc)

        pc.is_single_table = len(query.tables) == 1

        self.logger.debug(f'query classifiers: [{pc}]')

        self.query_table_node_map[query.query] = dict()

        for table, node_list in ctx.table_node_map.items():
            self.query_table_node_map[query.query].setdefault(table, list()).extend(node_list)

        self.query_context_map[query.query] = QueryContext(query, pc)

    def get_node_query(self, node):
        return self.node_context_map[id(node)].get_query()

    def get_node_parent_query(self, node):
        return self.node_context_map[id(node)].get_parent_query()

    def get_node_query_str(self, node):
        return self.get_node_query(node).query

    def get_node_plan_tree(self, node):
        return self.node_context_map[id(node)].get_plan_tree()

    def get_node_classifiers(self, node):
        return self.node_context_map[id(node)].node_classifiers

    @staticmethod
    def get_per_row_cost(node):
        return ((float(node.total_cost) - float(node.startup_cost))
                / (float(node.plan_rows) if float(node.plan_rows) else 1))

    @staticmethod
    def get_per_row_time(node):
        return ((float(node.total_ms) - float(node.startup_ms))
                / (float(node.rows) if float(node.rows) else 1))

    def get_node_width(self, node):
        return (0 if self.is_no_project_query(self.get_node_query(node).query)
                else int(self.node_context_map[id(node)].node_width))

    def get_columns_in_query(self, query_str):
        return self.query_context_map[query_str].get_columns()

    def has_no_condition(self, node):
        return self.get_node_classifiers(node).has_no_condition

    def has_no_local_filtering(self, node):
        nc = self.get_node_classifiers(node)
        return not nc.has_local_filter and not nc.has_rows_removed_by_recheck

    def has_only_simple_condition(self, node, index_cond_only=False, index_key_prefix_only=False):
        nc = self.get_node_classifiers(node)
        if (nc.has_partial_aggregate
            or not any([nc.has_index_access_cond,
                        nc.has_scan_filter,
                        nc.has_tfbr_filter])):
            return False
        conds = list()

        if node.index_name:
            conds += [node.get_index_cond()]
            if index_key_prefix_only:
                key_cols, _ = self.get_index_columns(node.index_name)
                if not self.get_expression_analyzer(*conds).has_key_prefix_cond_only(key_cols):
                    return False
        if not index_cond_only:
            conds += [node.get_remote_filter(), node.get_remote_tfbr_filter()]
        elif nc.has_scan_filter or nc.has_tfbr_filter:
            return False
        cond_str = ' AND '.join(filter(lambda cond: bool(cond), conds))
        return self.get_expression_analyzer(cond_str).is_simple_expr()

    def has_only_scan_filter_condition(self, node):
        nc = self.get_node_classifiers(node)
        return (not any([nc.has_partial_aggregate,
                         nc.has_index_access_cond,
                         nc.has_local_filter])
                and any([nc.has_scan_filter,
                         nc.has_tfbr_filter]))

    def has_partial_aggregate(self, node):
        return self.get_node_classifiers(node).has_partial_aggregate

    def get_table_row_count(self, table_name):
        if (nrows := self.table_row_map.get(table_name, -1)) < 0:
            self.logger.warn(f'{table_name}: table row count unavailable')
        return float(nrows)

    def get_table_column_position(self, table_name, column_name):
        cname = f'{table_name}:{column_name}'
        if (pos := self.column_position_map.get(cname, -1)) < 0:
            self.logger.warn(f'{cname}: table column position unavailable')
        return float(pos)

    @staticmethod
    def get_index_columns(index_name):
        # TODO: load the index key columns and save them into the .json
        key_cols = list()
        inc_cols = list()
        if index_name in ('t1000000m_pkey', 't1000000c10_pkey'):
            key_cols = list(['c0'])
        elif index_name.endswith('_pkey'):
            key_cols = list(['c1'])
        elif (m := index_key_extraction_pattern.match(index_name)):
            key_cols = packed_column_list_pattern.findall(m.group('key'))
            inc_cols = packed_column_list_pattern.findall(m.group('inc') or '')
        return key_cols, inc_cols

    def get_column_position(self, table_name, index_name, column_name):
        if index_name:
            key_cols, inc_cols = self.get_index_columns(index_name)
            if column_name in key_cols:
                return key_cols.index(column_name) + 1
            if column_name in inc_cols:
                return len(key_cols) + inc_cols.index(column_name) + 1
            return -1
        return self.get_table_column_position(table_name, column_name)

    def get_single_column_query_column(self, node):
        return self.get_columns_in_query(self.get_node_query_str(node))[0]

    def get_single_column_query_column_position(self, node):
        if not isinstance(node, ScanNode):
            return -1
        column_name = self.get_single_column_query_column(node)
        return self.get_column_position(node.table_name, node.index_name, column_name)

    def get_single_column_node_normalized_eq_cond_value(self, node):
        if not isinstance(node, ScanNode):
            return None
        ea = self.get_expression_analyzer(node.get_search_condition_str())
        if ea.is_simple_expr() and ea.simple_comp_exprs == 1:
            prop = ea.prop_list[0]
            if prop['op'] == '=':
                cst = column_stats_map.get(f'{node.table_name}.{prop["column"]}')
                return cst.normalize_value(int(prop['rhs']))
        return None

    def get_plan_classifiers(self, query_str):
        return self.query_context_map[query_str].pc

    def is_single_table_query(self, query_str):
        return self.get_plan_classifiers(query_str).is_single_table

    def is_no_project_query(self, query_str):
        return (query_str.lower().startswith('select 0 from')
                or query_str.lower().startswith('select count(*) from'))

    def has_no_filter_indexscan(self, query_str):
        return self.get_plan_classifiers(query_str).has_no_filter_index

    def has_scan_filter_indexscan(self, query_str):
        return self.get_plan_classifiers(query_str).has_scan_filter_index

    def has_local_filter(self, query_str):
        return self.get_plan_classifiers(query_str).has_local_filter

    def has_aggregate(self, query_str):
        return self.get_plan_classifiers(query_str).has_aggregate

    @staticmethod
    def wrap_expr(expr, len):
        line_start = 0
        lines = list()
        for m in re.finditer(r'\w+', expr):
            if m.end() - line_start > len:
                lines += [expr[line_start:m.start()]]
                line_start = m.start()

        lines += [expr[line_start:]]
        return lines

    def get_expression_analyzer(self, expr):
        ea = self.expr_analyzers.get(expr)
        if not ea:
            ea = ExpressionAnalyzer(expr)
            self.expr_analyzers[expr] = ea
        return ea

    def count_non_contiguous_literal_inlist_items(self, table, expr):
        num_item_list = list()
        for ea_prop in self.get_expression_analyzer(expr).prop_list:
            if literal_array := ea_prop.get('literal_array'):
                if (table and (cst := column_stats_map.get(f'{table}.{ea_prop["column"]}'))
                        and cst.ndv and cst.vmin):  # not empty table and not all-nulls
                    gap = cst.get_avg_value_distance()
                    ar = literal_array.split(',')
                    if gap and (int(ar[-1]) - int(ar[0]))/gap + 1 > len(ar):
                        ngaps = 0
                        for v1, v2 in pairwise(ar):
                            ngaps += int(bool(int(v2) - int(v1)))

                        num_item_list.append(ngaps + 1)

        return num_item_list

    def build_non_contiguous_literal_inlist_count_str(self, table, expr):
        num_item_list = self.count_non_contiguous_literal_inlist_items(table, expr)
        return ('x'.join(filter(lambda item: bool(item), map(str, sorted(num_item_list))))
                if num_item_list else '1')

    def has_literal_inlist_index_cond(self, node, single_in_list_only=False):
        if not isinstance(node, ScanNode):
            return False
        ea = self.get_expression_analyzer(node.get_index_cond())
        if single_in_list_only:
            return (len(ea.columns) == 1
                    and ea.simple_comp_exprs == 0
                    and ea.literal_in_lists == 1
                    and ea.bnl_in_lists == 0
                    and ea.complex_exprs == 0)
        return ea.literal_in_lists > 0

    def has_bnl_inlist_index_cond(self, node):
        return (self.get_expression_analyzer(node.get_index_cond()).bnl_in_lists > 0
                if isinstance(node, ScanNode) else False)

    def get_index_key_prefix_gaps(self, node):
        if (not isinstance(node, ScanNode)
                or not node.index_name
                or not (expr := node.get_index_cond())):
            return 1
        return index_prefix_gap_map.get(f'{node.index_name}:{expr}', 1)

    @staticmethod
    def gather_index_prefix_gap_query_parts(index_name, ea: ExpressionAnalyzer):
        if ((m := index_key_extraction_pattern.match(index_name))
                and (table := m.group("table"))
                and (key_cols := packed_column_list_pattern.findall(m.group('key')))):
            last_cond_key_pos = len(key_cols)
            for pos in reversed(range(last_cond_key_pos)):
                if key_cols[pos] in ea.columns:
                    last_cond_key_pos = pos
                    break

            gap_cols = list()
            cond = list()
            for col in key_cols[:last_cond_key_pos]:
                if col not in ea.columns:
                    gap_cols += [col]
                else:
                    for prop in ea.prop_list:
                        if prop['column'] == col:
                            op = prop['op']
                            if op != '=':
                                gap_cols += [col]
                                cond += [f'({col} {op} {prop["rhs"]})']

            if gap_cols:
                return gap_cols, table, cond

        return list(), '', list()

    @staticmethod
    def build_index_prefix_gap_query(gap_cols, table, cond):
        if not gap_cols or not table:
            return ''
        s = 'select count(*) from (select distinct '
        s += ', '.join(gap_cols)
        s += f' from {table}'
        if cond:
            s += ' where '
            s += ' and '.join(cond)

        s += ') v'
        return s

    def build_index_prefix_gap_queries(self):
        scan_node_list = list()
        for table_node_list_map in self.query_table_node_map.values():
            if nlist := (table_node_list_map.get('t1000000m')
                         or table_node_list_map.get('t1000000c10')):
                scan_node_list += filter(lambda node: (bool(node.index_name)
                                                       and node.get_index_cond()), nlist)

        querymap = dict()
        queries = set()
        for node in scan_node_list:
            indname = node.index_name
            expr = node.get_index_cond()
            ea = self.get_expression_analyzer(expr)
            indexpr = f'{indname}:{expr}'
            if indexpr not in querymap:
                if query := self.build_index_prefix_gap_query(
                        *self.gather_index_prefix_gap_query_parts(indname, ea)):
                    queries.add(query)
                    querymap[indexpr] = query

        self.logger.info(f'=== generating index prefix gap queries ({len(queries)}) ===')
        with open('report/index-gap-queries.sql', 'w') as gap_queries:
            lines = ';\n'.join(sorted(queries))
            lines += ';\n'
            gap_queries.write(lines)
        self.logger.info(f'=== generating index prefix gap map entries ({len(querymap)})===')
        with open('report/index-gap-map.in', 'w') as gap_map:
            lines = ''
            for indexpr, query in sorted(querymap.items()):
                lines += '    ("'
                lines += '"\n     "'.join(self.wrap_expr(indexpr, 72))
                lines += f'"): {{{query}}},\n'
            gap_map.write(lines)
