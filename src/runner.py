import argparse

from pyhocon import ConfigFactory

from config import Config, init_logger, ConnectionConfig, DDLStep
from database import DEFAULT_USERNAME, DEFAULT_PASSWORD, get_queries_from_previous_result
from reports.comparison import ComparisonReport
from reports.regression import RegressionReport
from reports.selectivity import SelectivityReport
from reports.taqo import TaqoReport

from workflow.scenario import Scenario
from utils import get_bool_from_str


def parse_ddls(ddl_ops):
    result = set()

    if ddl_ops == "none":
        return result

    if "create" in ddl_ops:
        result.add(DDLStep.CREATE)
    if "import" in ddl_ops:
        result.add(DDLStep.IMPORT)
    if "drop" in ddl_ops:
        result.add(DDLStep.DROP)
    if "analyze" in ddl_ops:
        result.add(DDLStep.ANALYZE)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Query Optimizer Testing framework for PostgreSQL compatible DBs')

    parser.add_argument('action',
                        help='Action to perform - collect or report')

    parser.add_argument('--config',
                        default="config/default.conf",
                        help='Configuration file path')

    parser.add_argument('--type',
                        help='Report type - taqo, regression, comparison or selectivity')

    # TAQO or Comparison
    parser.add_argument('--results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--pg-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')

    # Regression
    parser.add_argument('--v1-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--v2-results',
                        help='Path to previous execution results. May be used in regression and comparison reports')

    # Selectivity
    parser.add_argument('--default-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--default-analyze-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--ta-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--ta-analyze-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--stats-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--stats-analyze-results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')

    parser.add_argument('--ddl-prefix',
                        default="",
                        help='DDL file prefix (default empty, might be postgres)')

    parser.add_argument('--optimizations',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Evaluate optimizations for each query')
    parser.add_argument('--model',
                        default="simple",
                        help='Test model to use - complex, tpch, subqueries, any other custom model')

    parser.add_argument('--basic-multiplier',
                        default=10,
                        help='Basic model data multiplier (Default 10)')
    parser.add_argument('--source-path',
                        help='Path to yugabyte-db source code')
    parser.add_argument('--revision',
                        help='Git revision or path to release build')
    parser.add_argument('--ddls',
                        default="database,create,analyze,import,drop",
                        help='Model creation queries, comma separated: database,create,analyze,import,drop')

    parser.add_argument('--clean-db',
                        action=argparse.BooleanOptionalAction,
                        default=True,
                        help='Keep database after test')
    parser.add_argument('--allow-destroy-db',
                        action=argparse.BooleanOptionalAction,
                        default=True,
                        help='Allow to run yb-ctl/yugabyted destory')
    parser.add_argument('--clean-build',
                        action=argparse.BooleanOptionalAction,
                        default=True,
                        help='Build yb_build with --clean-force flag')

    parser.add_argument('--num-nodes',
                        default=0,
                        help='Number of nodes')

    parser.add_argument('--tserver-flags',
                        default=None,
                        help='Comma separated tserver flags')
    parser.add_argument('--master-flags',
                        default=None,
                        help='Comma separated master flags')

    parser.add_argument('--host',
                        default="127.0.0.1",
                        help='Target host IP for postgres compatible database')
    parser.add_argument('--port',
                        default=5433,
                        help='Target port for postgres compatible database')
    parser.add_argument('--username',
                        default=DEFAULT_USERNAME,
                        help='Username for connection')
    parser.add_argument('--password',
                        default=DEFAULT_PASSWORD,
                        help='Password for user for connection')
    parser.add_argument('--database',
                        default="taqo",
                        help='Target database in postgres compatible database')

    parser.add_argument('--enable-statistics',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Evaluate yb_enable_optimizer_statistics before running queries')
    parser.add_argument('--explain-clause',
                        default=None,
                        help='Explain clause that will be placed before query. Default "EXPLAIN"')
    parser.add_argument('--num-queries',
                        default=-1,
                        help='Number of queries to evaluate')
    parser.add_argument('--parametrized',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Run parametrized query instead of normal')

    parser.add_argument('--output',
                        help='Output JSON file name in report folder, [.json] will be added')

    parser.add_argument('--clear',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Clear logs directory')

    parser.add_argument('--yes',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Confirm test start')
    parser.add_argument('--verbose',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Enable DEBUG logging')

    args = parser.parse_args()

    configuration = ConfigFactory.parse_file(args.config)
    ddls = parse_ddls(args.ddls)

    config = Config(
        logger=init_logger("DEBUG" if args.verbose else "INFO"),

        source_path=args.source_path or configuration.get("source-path", None),
        num_nodes=int(args.num_nodes) or configuration.get("num-nodes", 3),

        revision=args.revision or None,
        tserver_flags=args.tserver_flags,
        master_flags=args.master_flags,
        clean_db=args.clean_db,
        allow_destroy_db=args.allow_destroy_db,
        clean_build=args.clean_build,

        connection=ConnectionConfig(host=args.host,
                                    port=args.port,
                                    username=args.username,
                                    password=args.password,
                                    database=args.database),

        model=args.model,
        output=args.output,
        ddls=ddls,
        ddl_prefix=args.ddl_prefix,
        with_optimizations=args.optimizations,

        enable_statistics=args.enable_statistics or get_bool_from_str(
            configuration.get("enable-statistics", False)),
        explain_clause=args.explain_clause or configuration.get("explain-clause", "EXPLAIN"),
        session_props=configuration.get("session-props", []),
        basic_multiplier=int(args.basic_multiplier),

        random_seed=configuration.get("random-seed", 2022),
        skip_percentage_delta=configuration.get("skip-percentage-delta", 0.05),
        skip_timeout_delta=configuration.get("skip-timeout-delta", 1),
        look_near_best_plan=configuration.get("look-near-best-plan", True),
        all_pairs_threshold=configuration.get("all-pairs-threshold", 3),

        num_queries=int(args.num_queries)
        if int(args.num_queries) > 0 else configuration.get("num-queries", -1),
        num_retries=configuration.get("num-retries", 5),
        num_warmup=configuration.get("num-warmup", 2),

        parametrized=args.parametrized,

        asciidoctor_path=configuration.get("asciidoctor_path", "asciidoc"),

        clear=args.clear)

    config.logger.info("------------------------------------------------------------")
    config.logger.info("Query Optimizer Testing Framework for Postgres compatible DBs")
    config.logger.info("")
    config.logger.info("Initial configuration:")
    for line in str(config).split("\n"):
        config.logger.info(line)
    config.logger.info("------------------------------------------------------------")

    if args.action == "collect":
        if args.output is None:
            print("ARGUMENTS VALIDATION ERROR: --output arg is required for collect task")
            exit(1)

        if args.yes:
            input("Validate configuration carefully and press Enter...")

        sc = Scenario(config)
        sc.evaluate()
    elif args.action == "report":
        if args.type == "taqo":
            yb_queries = get_queries_from_previous_result(args.results)
            pg_queries = get_queries_from_previous_result(
                args.pg_results) if args.pg_results else None

            TaqoReport.generate_report(yb_queries, pg_queries)
        elif args.type == "regression":
            report = RegressionReport()

            v1_queries = get_queries_from_previous_result(args.v1_results)
            v2_queries = get_queries_from_previous_result(args.v2_results)

            report.generate_report(v1_queries, v2_queries)
        elif args.type == "comparison":
            report = ComparisonReport()

            yb_queries = get_queries_from_previous_result(args.results)
            pg_queries = get_queries_from_previous_result(
                args.pg_results) if args.pg_results else None

            report.generate_report(yb_queries, pg_queries)
        elif args.type == "selectivity":
            report = SelectivityReport()

            default_queries = get_queries_from_previous_result(args.default_results)
            default_analyze_queries = get_queries_from_previous_result(args.default_analyze_results)
            ta_queries = get_queries_from_previous_result(args.ta_results)
            ta_analyze_queries = get_queries_from_previous_result(args.ta_analyze_results)
            stats_queries = get_queries_from_previous_result(args.stats_results)
            stats_analyze_queries = get_queries_from_previous_result(args.stats_analyze_results)

            report.generate_report(default_queries, default_analyze_queries, ta_queries,
                                   ta_analyze_queries, stats_queries, stats_analyze_queries)
        else:
            raise AttributeError(f"Unknown test type defined {config.test}")
