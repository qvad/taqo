import argparse

from pyhocon import ConfigFactory

from config import Config, init_logger, ConnectionConfig, ModelSteps
from database import DEFAULT_USERNAME, DEFAULT_PASSWORD
from tests.approach.scenario import ApproachTest
from tests.comparison.scenario import ComparisonTest
from tests.regression.scenario import RegressionTest
from tests.taqo.scenario import TaqoTest
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

    parser.add_argument('--config',
                        default="config/default.conf",
                        help='Configuration file path')

    parser.add_argument('--test',
                        default="taqo",
                        help='Type of test to evaluate - taqo (default) or regression')
    parser.add_argument('--model',
                        default="simple",
                        help='Test model to use - complex, tpch, subqueries, any other custom model')
    parser.add_argument('--previous_results_path',
                        help='Path to previous execution results. May be used in regression and comparison reports')
    parser.add_argument('--basic_multiplier',
                        default=10,
                        help='Basic model data multiplier (Default 10)')
    parser.add_argument('--yugabyte_code_path',
                        help='Code path to yugabyte-db repository')
    parser.add_argument('--revisions',
                        help='Comma separated git revisions or paths to release builds')

    parser.add_argument('--num_nodes',
                        default=0,
                        help='Number of nodes')

    parser.add_argument('--tserver_flags',
                        default=None,
                        help='Comma separated tserver flags')
    parser.add_argument('--master_flags',
                        default=None,
                        help='Comma separated master flags')

    parser.add_argument('--explain_clause',
                        default=None,
                        help='Explain clause that will be placed before query. Default "EXPLAIN"')

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
    parser.add_argument('--num_queries',
                        default=-1,
                        help='Number of queries to evaluate')
    parser.add_argument('--compare_with_pg',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Add compare with postgres to report')

    parser.add_argument('--model_creation',
                        default="create,import,teardown",
                        help='Model creation queries, comma separated: create, import, teardown')
    parser.add_argument('--destroy_database',
                        action=argparse.BooleanOptionalAction,
                        default=True,
                        help='Destroy database after test')
    parser.add_argument('--output',
                        default="output",
                        help='Output JSON file name in report folder, default: output [.json]')

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

    pg_connection = None
    if args.compare_with_pg or args.test == "comparison":
        if not configuration.get("postgres"):
            print("Compare with PG is enabled, but no postgres connection parameters specified")
            exit(1)

        pg_connection = ConnectionConfig(
            host=configuration["postgres.host"],
            port=configuration["postgres.port"],
            username=configuration["postgres.username"],
            password=configuration["postgres.password"],
            database=configuration["postgres.database"]
        )

    config = Config(
        logger=init_logger("DEBUG" if args.verbose else "INFO"),

        # configuration file properties
        yugabyte_code_path=args.yugabyte_code_path or configuration.get("yugabyte_code_path"),
        previous_results_path=args.previous_results_path,
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
        num_retries=configuration.get("num_retries", 5),
        num_warmup=configuration.get("num_warmup", 5),

        asciidoctor_path=configuration.get("asciidoctor_path", "asciidoc"),

        # args properties
        revisions_or_paths=args.revisions.split(",") if args.revisions else [],
        tserver_flags=args.tserver_flags,
        master_flags=args.master_flags,

        yugabyte=ConnectionConfig(host=args.host,
                                  port=args.port,
                                  username=args.username,
                                  password=args.password,
                                  database=args.database),
        postgres=pg_connection,

        compare_with_pg=args.compare_with_pg,
        model_creation=model_creation_args,
        destroy_database=args.destroy_database,
        output=args.output,

        enable_statistics=args.enable_statistics or get_bool_from_str(
            configuration.get("enable_statistics", False)),
        explain_clause=args.explain_clause or configuration.get("explain_clause", "EXPLAIN"),
        session_props=configuration.get("session_props", []),
        session_props_v1=configuration.get("session_props_v1", []),
        session_props_v2=configuration.get("session_props_v2", []),

        test=args.test,
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

    if config.test == "taqo":
        test = TaqoTest()
    elif config.test == "regression":
        test = RegressionTest()
    elif config.test == "comparison":
        test = ComparisonTest()
    elif config.test == "approach":
        test = ApproachTest()
    else:
        raise AttributeError(f"Unknown test type defined {config.test}")

    test.evaluate()
