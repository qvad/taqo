import argparse

from pyhocon import ConfigFactory

from config import Config, init_logger, ConnectionConfig, ModelSteps
from database import DEFAULT_USERNAME, DEFAULT_PASSWORD, get_queries_from_previous_result
from tests.reports.comparison import ComparisonReport
from tests.reports.regression import RegressionReport
from tests.reports.selectivity import SelectivityReport
from tests.reports.taqo import TaqoReport

from tests.scenario import Scenario
from utils import get_bool_from_str


def parse_model_creation(model_creation_arg):
    result = set()

    if model_creation_arg == "none":
        return result

    if "create" in model_creation_arg:
        result.add(ModelSteps.CREATE)
    if "import" in model_creation_arg:
        result.add(ModelSteps.IMPORT)
    if "teardown" in model_creation_arg:
        result.add(ModelSteps.TEARDOWN)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Query Optimizer Testing framework for PostgreSQL compatible DBs')

    parser.add_argument('action',
                        help='Action to perform - collect or report')

    parser.add_argument('--config',
                        default="config/default.conf",
                        help='Configuration file path')

    parser.add_argument('--report',
                        help='Report type - taqo, regression, comparison or selectivity')

    # TAQO or Comparison
    parser.add_argument('--results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--pg_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')

    # Regression
    parser.add_argument('--v1_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--v2_results',
                        help='Path to previous execution results. May be used in regression and comparison reports')

    # Selectivity
    parser.add_argument('--default_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--default_analyze_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--ta_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--ta_analyze_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--stats_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--stats_analyze_results',
                        default=None,
                        help='Path to previous execution results. May be used in regression and comparison reports')


    parser.add_argument('--ddl_prefix',
                        default="",
                        help='DDL file prefix (default empty, might be postgres)')

    parser.add_argument('--optimizations',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Evaluate optimizations for each query')
    parser.add_argument('--model',
                        default="simple",
                        help='Test model to use - complex, tpch, subqueries, any other custom model')

    parser.add_argument('--compare_with_pg',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Add compare with postgres to report')
    parser.add_argument('--basic_multiplier',
                        default=10,
                        help='Basic model data multiplier (Default 10)')
    parser.add_argument('--yugabyte_code_path',
                        help='Code path to yugabyte-db repository')
    parser.add_argument('--revision',
                        help='Git revision or path to release build')
    parser.add_argument('--model_creation',
                        default="create,import,teardown",
                        help='Model creation queries, comma separated: create, import, teardown')
    parser.add_argument('--destroy_database',
                        action=argparse.BooleanOptionalAction,
                        default=True,
                        help='Destroy database after test')

    parser.add_argument('--num_nodes',
                        default=0,
                        help='Number of nodes')

    parser.add_argument('--tserver_flags',
                        default=None,
                        help='Comma separated tserver flags')
    parser.add_argument('--master_flags',
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
                        default="postgres",
                        help='Target database in postgres compatible database')

    parser.add_argument('--enable_statistics',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Evaluate yb_enable_optimizer_statistics before running queries')
    parser.add_argument('--explain_clause',
                        default=None,
                        help='Explain clause that will be placed before query. Default "EXPLAIN"')
    parser.add_argument('--num_queries',
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

    parser.add_argument('--verbose',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Enable DEBUG logging')

    args = parser.parse_args()

    configuration = ConfigFactory.parse_file(args.config)
    model_creation_args = parse_model_creation(args.model_creation)

    config = Config(
        logger=init_logger("DEBUG" if args.verbose else "INFO"),

        ddl_prefix=args.ddl_prefix,
        with_optimizations=args.optimizations,

        # configuration file properties
        yugabyte_code_path=args.yugabyte_code_path or configuration.get("yugabyte_code_path", None),
        num_nodes=int(args.num_nodes) or configuration.get("num_nodes", 3),

        random_seed=configuration.get("random_seed", 2022),
        use_allpairs=configuration.get("use_allpairs", True),

        skip_table_scan_hints=configuration.get("skip_table_scan_hints", False),
        skip_percentage_delta=configuration.get("skip_percentage_delta", 0.05),
        skip_timeout_delta=configuration.get("skip_timeout_delta", 1),
        look_near_best_plan=configuration.get("look_near_best_plan", True),
        max_optimizations=configuration.get("max_optimizations", 1000),

        num_queries=int(args.num_queries)
        if int(args.num_queries) > 0 else configuration.get("num_queries", -1),
        parametrized=args.parametrized,
        num_retries=configuration.get("num_retries", 5),
        num_warmup=configuration.get("num_warmup", 5),

        asciidoctor_path=configuration.get("asciidoctor_path", "asciidoc"),

        # args properties
        revision=args.revision if args.revision else None,
        tserver_flags=args.tserver_flags,
        master_flags=args.master_flags,

        yugabyte=ConnectionConfig(host=args.host,
                                  port=args.port,
                                  username=args.username,
                                  password=args.password,
                                  database=args.database),

        compare_with_pg=args.compare_with_pg,
        model_creation=model_creation_args,
        destroy_database=args.destroy_database,
        output=args.output,

        enable_statistics=args.enable_statistics or get_bool_from_str(
            configuration.get("enable_statistics", False)),
        explain_clause=args.explain_clause or configuration.get("explain_clause", "EXPLAIN"),
        session_props=configuration.get("session_props", []),

        model=args.model,
        basic_multiplier=int(args.basic_multiplier),

        clear=args.clear,
    )

    config.logger.info("------------------------------------------------------------")
    config.logger.info("Query Optimizer Testing Framework for Postgres compatible DBs")
    config.logger.info("")
    config.logger.info("Initial configuration:")
    for line in str(config).split("\n"):
        config.logger.info(line)
    config.logger.info("------------------------------------------------------------")

    if args.action == "collect":
        sc = Scenario(config)
        sc.evaluate()
    elif args.action == "report":
        if args.report == "taqo":
            report = TaqoReport()

            yb_queries = get_queries_from_previous_result(args.results)
            pg_queries = get_queries_from_previous_result(args.pg_results) if args.pg_results else None

            report.generate_report(yb_queries)
        elif args.report == "regression":
            report = RegressionReport()

            v1_queries = get_queries_from_previous_result(args.v1_results)
            v2_queries = get_queries_from_previous_result(args.v2_results)

            report.generate_report(v1_queries, v2_queries)
        elif args.report == "comparison":
            report = ComparisonReport()

            yb_queries = get_queries_from_previous_result(args.results)
            pg_queries = get_queries_from_previous_result(args.pg_results) if args.pg_results else None

            report.generate_report(yb_queries, pg_queries)
        elif args.report == "approach":
            report = SelectivityReport()

            default_queries = get_queries_from_previous_result(args.default_results)
            default_analyze_queries = get_queries_from_previous_result(args.default_analyze_results)
            ta_queries = get_queries_from_previous_result(args.ta_results)
            ta_analyze_queries = get_queries_from_previous_result(args.ta_analyze_results)
            stats_queries = get_queries_from_previous_result(args.stats_results)
            stats_analyze_queries = get_queries_from_previous_result(args.stats_analyze_results)

            report.generate_report(default_queries, default_analyze_queries, ta_queries, ta_analyze_queries, stats_queries, stats_analyze_queries)
        else:
            raise AttributeError(f"Unknown test type defined {config.test}")
