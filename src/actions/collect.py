import subprocess

import psycopg2
from tqdm import tqdm

from actions.collects.pg_unit import PgUnitGenerator
from config import Config, DDLStep
from models.factory import get_test_model
from objects import EXPLAIN, ExplainFlags
from utils import evaluate_sql, calculate_avg_execution_time, get_md5, allowed_diff, extract_execution_time_from_analyze


class CollectAction:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger
        self.sut_database = self.config.database

    def start_db(self):
        self.logger.info(f"Initializing {self.sut_database.__class__.__name__} DB")

        commit_hash = self.config.revision

        self.sut_database.change_version_and_compile(commit_hash)
        self.sut_database.stop_database()
        self.sut_database.destroy()
        self.sut_database.start_database()

        return self.get_commit_message(commit_hash)

    def get_commit_message(self, commit_hash):
        if commit_hash:
            output = str(subprocess.check_output(
                f"echo `git log -n 1 --pretty=format:%s {commit_hash}`",
                cwd=self.config.source_path,
                shell=True)).rstrip('\n')
            return f"{output} ({commit_hash})"
        else:
            return ""

    def evaluate(self):
        loader = self.config.database.get_results_loader()

        self.start_db()
        try:
            self.sut_database.create_test_database()

            self.sut_database.establish_connection(self.config.connection.database)

            loq = self.config.database.get_list_queries()
            with self.sut_database.connection.conn.cursor() as cur:
                loq.db_version, loq.git_message = self.sut_database.get_revision_version(cur)

            loq.model_queries, loq.queries = self.run_ddl_and_testing_queries(
                self.sut_database.connection.conn, self.config.with_optimizations)

            loq.config = str(self.config)

            self.logger.info(f"Storing results to report/{self.config.output}")
            loader.store_queries_to_file(loq, self.config.output)
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            if self.config.clean_db:
                self.sut_database.stop_database()

    def run_ddl_and_testing_queries(self,
                                    connection,
                                    evaluate_optimizations=False):
        try:
            model = get_test_model()

            created_tables, \
                non_catalog_tables, \
                teardown_queries, \
                create_queries, \
                analyze_queries, \
                import_queries = model.create_tables(connection)

            model_queries = teardown_queries + create_queries + analyze_queries + import_queries
            queries = model.get_queries(created_tables)

            if DDLStep.COMPACT in self.config.ddls:
                self.sut_database.run_compaction(tables=non_catalog_tables)
        except Exception as e:
            self.logger.exception("Failed to evaluate DDL queries", e)
            exit(1)

        connection.autocommit = False
        self.evaluate_testing_queries(connection, queries, evaluate_optimizations)

        PgUnitGenerator().generate_postgres_unit_tests(teardown_queries,
                                                       create_queries,
                                                       queries)

        return model_queries, queries

    def evaluate_testing_queries(self, conn, queries, evaluate_optimizations):
        counter = 1
        for original_query in queries:
            with conn.cursor() as cur:
                try:
                    self.logger.info(
                        f"Evaluating query with hash {original_query.query_hash} [{counter}/{len(queries)}]")
                    self.sut_database.set_query_timeout(cur, self.config.test_query_timeout)

                    # get default execution plan
                    self.sut_database.prepare_query_execution(cur, original_query)
                    evaluate_sql(cur, original_query.get_explain(EXPLAIN))
                    default_execution_plan = self.config.database.get_execution_plan(
                        '\n'.join(str(item[0]) for item in cur.fetchall()))
                    conn.rollback()

                    # store default execution plan if query execution will fail
                    original_query.execution_plan = default_execution_plan

                    # get costs off execution plan
                    self.sut_database.prepare_query_execution(cur, original_query)
                    evaluate_sql(cur, original_query.get_explain(EXPLAIN, [ExplainFlags.COSTS_OFF]))
                    original_query.cost_off_explain = self.config.database.get_execution_plan(
                        '\n'.join(str(item[0]) for item in cur.fetchall()))
                    conn.rollback()

                    self.define_min_execution_time(conn, cur, original_query)

                    if self.config.plans_only:
                        original_query.execution_time_ms = default_execution_plan.get_estimated_cost()
                    else:
                        query_str = original_query.get_explain(EXPLAIN, options=[ExplainFlags.ANALYZE]) \
                            if self.config.server_side_execution else None
                        calculate_avg_execution_time(cur, original_query, self.sut_database,
                                                     query_str=query_str,
                                                     num_retries=int(self.config.num_retries),
                                                     connection=conn)

                    if evaluate_optimizations and "dml" not in original_query.optimizer_tips.tags:
                        self.logger.debug("Evaluating optimizations...")
                        self.evaluate_optimizations(conn, cur, original_query)

                        if not self.config.plans_only:
                            self.validate_result_hash(original_query)
                            self.validate_execution_time(original_query)

                except psycopg2.Error as pe:
                    # do not raise exception
                    self.logger.exception(f"{original_query}\nFailed because of {pe}")
                except Exception as e:
                    self.logger.info(original_query)
                    raise e
                finally:
                    counter += 1

            conn.rollback()

    def validate_result_hash(self, original_query):
        result_hash = original_query.result_hash
        for optimization in original_query.optimizations:
            if optimization.result_hash and result_hash != optimization.result_hash:
                cardinality_equality = "=" if original_query.result_cardinality == optimization.result_cardinality else "!="

                if "now()" in original_query.query.lower():
                    # todo fixing result_hash for queries with function calls
                    optimization.query_hash = original_query.result_hash
                    continue

                self.config.has_failures = True
                self.logger.exception(f"INCONSISTENT RESULTS!\n"
                                      f"Validation: {original_query.result_hash} != {optimization.result_hash}\n"
                                      f"Cardinality: {original_query.result_cardinality} {cardinality_equality} {optimization.result_cardinality}\n"
                                      f"Reproducer original: {original_query.query}\n"
                                      f"Reproducer optimization: /*+ {optimization.explain_hints} */ {optimization.query}\n")

                if self.config.exit_on_fail:
                    exit(1)

    def validate_execution_time(self, original_query):
        explain_execution_time = extract_execution_time_from_analyze(original_query.execution_plan.full_str)
        avg_execution_time = original_query.execution_time_ms

        if explain_execution_time and (explain_execution_time > avg_execution_time and
                not allowed_diff(self.config, avg_execution_time, explain_execution_time)):
            self.config.has_warnings = True
            self.logger.warning(f"WARNING!\n"
                                f"ANALYZE query execution time is too large:\n"
                                f"Execution times (explain vs avg): {explain_execution_time} < {avg_execution_time}\n"
                                f"Query: {original_query.query}\n")

    def define_min_execution_time(self, conn, cur, original_query):
        if self.config.baseline_results:
            if baseline_result := \
                    self.config.baseline_results.find_query_by_hash(original_query.query_hash):
                # get the best optimization from baseline run
                best_optimization = baseline_result.get_best_optimization(self.config)
                query_str = best_optimization.get_explain(EXPLAIN, options=[ExplainFlags.ANALYZE]) \
                    if self.config.server_side_execution else None
                calculate_avg_execution_time(cur,
                                             best_optimization,
                                             self.sut_database,
                                             query_str=query_str,
                                             num_retries=int(self.config.num_retries),
                                             connection=conn)
                self.set_query_timeout_based_on_previous_execution(cur,
                                                                   best_optimization.execution_time_ms,
                                                                   original_query)
            else:
                self.sut_database.set_query_timeout(cur, self.config.test_query_timeout)
        else:
            self.sut_database.set_query_timeout(cur, self.config.test_query_timeout)

    def evaluate_optimizations(self, connection, cur, original_query):
        # build all possible optimizations
        database = self.config.database
        list_of_optimizations = database.get_list_optimizations(original_query)

        self.logger.debug(f"{len(list_of_optimizations)} optimizations generated")
        progress_bar = tqdm(list_of_optimizations)
        duplicates = 0
        timed_out = 0
        min_execution_time = original_query.execution_time_ms \
            if original_query.execution_time_ms > 0 else (self.config.test_query_timeout * 1000)
        original_query.optimizations = []
        execution_plans_checked = set()

        for optimization in progress_bar:
            # in case of enable statistics enabled
            # we can get failure here and throw timeout
            original_query.optimizations.append(optimization)

            # set maximum execution time if this is first query,
            # or we are evaluating queries near best execution time
            if self.config.look_near_best_plan or len(original_query.optimizations) == 1:
                self.set_query_timeout_based_on_previous_execution(cur, min_execution_time, original_query)

            self.try_to_get_default_explain_hints(cur, optimization, original_query)

            # check that execution plan is unique
            evaluate_sql(cur, optimization.get_explain(EXPLAIN, options=[ExplainFlags.COSTS_OFF]))
            optimization.cost_off_explain = database.get_execution_plan(
                '\n'.join(str(item[0]) for item in cur.fetchall())
            )
            exec_plan_md5 = get_md5(optimization.cost_off_explain.get_clean_plan())
            not_unique_plan = exec_plan_md5 in execution_plans_checked
            execution_plans_checked.add(exec_plan_md5)
            query_str = optimization.get_explain(EXPLAIN, options=[ExplainFlags.ANALYZE]) \
                if self.config.server_side_execution else None

            if not_unique_plan:
                duplicates += 1
            else:
                try:
                    self.sut_database.prepare_query_execution(cur, optimization)
                    evaluate_sql(cur, optimization.get_explain(EXPLAIN))
                    default_execution_plan = database.get_execution_plan(
                        '\n'.join(str(item[0]) for item in cur.fetchall())
                    )
                except psycopg2.errors.QueryCanceled as e:
                    # failed by timeout in getting EXPLAIN - issue
                    self.logger.exception(f"Getting default execution plan failed with {e}")
                    continue

                if self.config.plans_only:
                    original_query.execution_plan = default_execution_plan
                    original_query.execution_time_ms = default_execution_plan.get_estimated_cost()
                elif not calculate_avg_execution_time(
                        cur,
                        optimization,
                        self.sut_database,
                        query_str=query_str,
                        num_retries=int(self.config.num_retries),
                        connection=connection):
                    timed_out += 1

            # get new minimum execution time
            if 0 < optimization.execution_time_ms < min_execution_time:
                min_execution_time = optimization.execution_time_ms

            progress_bar.set_postfix(
                {'skipped': f"(dp: {duplicates}, to: {timed_out})", 'min_time_ms': "{:.2f}".format(min_execution_time)})

        return list_of_optimizations

    def set_query_timeout_based_on_previous_execution(self, cur, min_execution_time, original_query):
        optimizer_query_timeout = \
            (original_query.optimizer_tips and original_query.optimizer_tips.max_timeout) or \
            f"{int(min_execution_time / 1000) + int(self.config.skip_timeout_delta)}"
        self.sut_database.set_query_timeout(cur, optimizer_query_timeout)

    def try_to_get_default_explain_hints(self, cur, optimization, original_query):
        if not original_query.explain_hints:
            if optimization.execution_plan is None:
                evaluate_sql(cur, optimization.get_explain(EXPLAIN))

                execution_plan = self.config.database.get_execution_plan('\n'.join(
                    str(item[0]) for item in cur.fetchall()))
            else:
                execution_plan = optimization.execution_plan

            if original_query.compare_plans(execution_plan) and original_query.tips_looks_fair(
                    optimization):
                # store execution plan hints from optimization
                original_query.explain_hints = optimization.explain_hints
