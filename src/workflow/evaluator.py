from difflib import SequenceMatcher

import psycopg2
from tqdm import tqdm

from database import ENABLE_STATISTICS_HINT, ExecutionPlan, Query, ListOfOptimizations
from models.factory import get_test_model
from utils import evaluate_sql, get_optimizer_score_from_plan, calculate_avg_execution_time, \
    get_md5, allowed_diff


class QueryEvaluator:
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger

    def evaluate(self,
                 connection,
                 evaluate_optimizations=False):
        queries = []
        try:
            model = get_test_model()
            created_tables, model_queries = model.create_tables(connection)
            queries = model.get_queries(created_tables)
        except Exception as e:
            self.logger.exception("Failed to evaluate DDL queries", e)
            exit(1)

        self.evaluate_queries_against_yugabyte(connection, queries, evaluate_optimizations)

        return queries

    def evaluate_queries_against_yugabyte(self, conn, queries, evaluate_optimizations):
        with conn.cursor() as cur:
            counter = 1

            for query in self.config.session_props:
                evaluate_sql(cur, query)

            if self.config.enable_statistics:
                self.logger.debug("Enable yb_enable_optimizer_statistics flag")

                evaluate_sql(cur, ENABLE_STATISTICS_HINT)

            for original_query in queries:
                try:
                    evaluate_sql(cur, f"SET statement_timeout = '{self.config.max_query_timeout}s'")

                    short_query = original_query.query.replace('\n', '')[:40]
                    self.logger.info(
                        f"Evaluating query {short_query}... [{counter}/{len(queries)}]")

                    try:
                        evaluate_sql(cur, original_query.get_explain())
                        original_query.execution_plan = ExecutionPlan('\n'.join(
                            str(item[0]) for item in cur.fetchall()))
                        original_query.optimizer_score = get_optimizer_score_from_plan(
                            original_query.execution_plan)
                    except psycopg2.errors.QueryCanceled:
                        original_query.execution_plan = ExecutionPlan('')
                        original_query.optimizer_score = -1

                    calculate_avg_execution_time(cur, original_query,
                                                 num_retries=int(self.config.num_retries))

                    if evaluate_optimizations:
                        self.logger.debug("Evaluating optimizations...")
                        self.evaluate_optimizations(cur, original_query)
                        self.plan_heatmap(original_query)

                except psycopg2.Error as pe:
                    # do not raise exception
                    self.logger.exception(f"{original_query}\nFailed because of {pe}")
                except Exception as e:
                    self.logger.info(original_query)
                    raise e
                finally:
                    counter += 1

    def evaluate_optimizations(self, cur, original_query):
        # build all possible optimizations
        list_of_optimizations = ListOfOptimizations(
            self.config, original_query) \
            .get_all_optimizations()

        self.logger.debug(f"{len(list_of_optimizations)} optimizations generated")
        progress_bar = tqdm(list_of_optimizations)
        num_skipped = 0
        min_execution_time = original_query.execution_time_ms
        original_query.optimizations = []
        execution_plans_checked = set()

        for optimization in progress_bar:
            # in case of enable statistics enabled
            # we can get failure here and throw timeout
            original_query.optimizations.append(optimization)

            # set maximum execution time if this is first query,
            # or we are evaluating queries near best execution time
            if self.config.look_near_best_plan or len(original_query.optimizations) == 1:
                optimizer_query_timeout = \
                    (original_query.optimizer_tips and original_query.optimizer_tips.max_timeout) or \
                    f"{int(min_execution_time / 1000) + int(self.config.skip_timeout_delta)}s"

                self.logger.debug(f"Setting query timeout to {optimizer_query_timeout} seconds")

                evaluate_sql(cur, f"SET statement_timeout = '{optimizer_query_timeout}'")

            self.try_to_get_default_explain_hints(cur, optimization, original_query)

            try:
                evaluate_sql(cur, optimization.get_explain())
            except psycopg2.errors.QueryCanceled as e:
                # failed by timeout - it's ok just skip optimization
                self.logger.debug(f"Getting execution plan failed with {e}")

                num_skipped += 1
                optimization.execution_time_ms = 0
                optimization.execution_plan = ExecutionPlan("")
                optimization.optimizer_score = 0
                continue

            optimization.execution_plan = ExecutionPlan('\n'.join(
                str(item[0]) for item in cur.fetchall()))

            exec_plan_md5 = get_md5(optimization.execution_plan.get_clean_plan())
            not_unique_plan = exec_plan_md5 in execution_plans_checked
            execution_plans_checked.add(exec_plan_md5)

            optimization.optimizer_score = get_optimizer_score_from_plan(
                optimization.execution_plan)

            if not_unique_plan or not calculate_avg_execution_time(
                    cur,
                    optimization,
                    num_retries=int(self.config.num_retries)):
                num_skipped += 1

            # get new minimum execution time
            if optimization.execution_time_ms != 0 and \
                    optimization.execution_time_ms < min_execution_time:
                min_execution_time = optimization.execution_time_ms

            progress_bar.set_postfix(
                {'skipped': num_skipped, 'min_execution_time_ms': min_execution_time})

        return list_of_optimizations

    def plan_heatmap(self, query: Query):
        plan_heatmap = {line_id: {'weight': 0, 'str': execution_plan_line}
                        for line_id, execution_plan_line in
                        enumerate(query.execution_plan.get_no_cost_plan().split("->"))}

        best_optimization = query.get_best_optimization(self.config)
        for optimization in query.optimizations:
            if allowed_diff(self.config, best_optimization.execution_time_ms,
                            optimization.execution_time_ms):
                no_cost_plan = optimization.execution_plan.get_no_cost_plan()
                for plan_line in plan_heatmap.values():
                    for optimization_line in no_cost_plan.split("->"):
                        if SequenceMatcher(
                                a=optimization.execution_plan.get_no_tree_plan_str(plan_line['str']),
                                b=optimization.execution_plan.get_no_tree_plan_str(optimization_line)
                        ).ratio() > 0.9:
                            plan_line['weight'] += 1

        query.execution_plan_heatmap = plan_heatmap

    def try_to_get_default_explain_hints(self, cur, optimization, original_query):
        if not original_query.explain_hints:
            if self.config.enable_statistics or optimization.execution_plan is None:
                evaluate_sql(cur, optimization.get_heuristic_explain())

                execution_plan = ExecutionPlan('\n'.join(
                    str(item[0]) for item in cur.fetchall()))
            else:
                execution_plan = optimization.execution_plan

            if original_query.compare_plans(execution_plan) and original_query.tips_looks_fair(
                    optimization):
                # store execution plan hints from optimization
                original_query.explain_hints = optimization.explain_hints
