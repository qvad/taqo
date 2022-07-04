import argparse

from src.config import Config
from src.tests.regression import evaluate_regression
from src.tests.taqo import evaluate_taqo

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Query Optimizer Testing framework for PostgreSQL compatible DBs')

    parser.add_argument('--host',
                        default="localhost",
                        help='Target host IP for postgres compatible database')
    parser.add_argument('--port',
                        default=5433,
                        help='Target port for postgres compatible database')
    parser.add_argument('--username',
                        default="postgres",
                        help='Username for connection')
    parser.add_argument('--password',
                        default="postgres",
                        help='Password for user for connection')
    parser.add_argument('--database',
                        default="postgres",
                        help='Target database in postgres compatible database')

    parser.add_argument('--enable-statistics',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Evaluate yb_enable_optimizer_statistics before running queries')
    parser.add_argument('--explain-analyze',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Evaluate EXPLAIN ANALYZE instead of EXPLAIN')

    parser.add_argument('--test',
                        default="taqo",
                        help='Type of test to evaluate - taqo (default) or regression')
    parser.add_argument('--model',
                        default="simple",
                        help='Test model to use - simple (default) or tpch')

    parser.add_argument('--skip-model-creation',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Skip model creation queries')
    parser.add_argument('--skip-table-scan-hints',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Skip table scan hinting')

    parser.add_argument('--num-queries',
                        default=0,
                        help='Number of queries for default model')
    parser.add_argument('--num-retries',
                        default=5,
                        help='Number of retries')
    parser.add_argument('--skip-timeout-delta',
                        default=2,
                        help='Timeout delta for optimized query')
    parser.add_argument('--max-optimizations',
                        default=300,
                        help='Maximum number of allowed optimizations (default 300)')

    parser.add_argument('--asciidoctor-path',
                        default="asciidoctor",
                        help='Full path to asciidoc command (default asciidoctor)')

    parser.add_argument('--verbose',
                        action=argparse.BooleanOptionalAction,
                        default=False,
                        help='Enable extra logging')

    args = parser.parse_args()

    config = Config(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        database=args.database,

        enable_statistics=args.enable_statistics,
        explain_analyze=args.explain_analyze,

        test=args.test,
        model=args.model,

        skip_model_creation=args.skip_model_creation,
        skip_table_scan_hints=args.skip_table_scan_hints,

        num_queries=args.num_queries,
        num_retries=args.num_retries,
        skip_timeout_delta=args.skip_timeout_delta,
        max_optimizations=args.max_optimizations,

        asciidoctor_path=args.asciidoctor_path,
        verbose=args.verbose,
    )

    if args.test == "taqo":
        evaluate_taqo()
    elif args.test == "regression":
        evaluate_regression()
