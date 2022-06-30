import argparse

from src.tests.regression import evaluate_regression
from src.tests.taqo import evaluate_taqo


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Query Optimizer Testing framework for Posgtres-compatible DBs.')
    parser.add_argument('--host',
                        default="localhost",
                        help='Target host IP for YugabyteDB')
    parser.add_argument('--port',
                        default=5433,
                        help='Target port for YugabyteDB')
    parser.add_argument('--username',
                        default="postgres",
                        help='Username for connection')
    parser.add_argument('--password',
                        default="postgres",
                        help='Password for user for connection')
    parser.add_argument('--database',
                        default="postgres",
                        help='Target database in YugabyteDB')

    parser.add_argument('--test',
                        default="taqo",
                        help='Type of test to evaluate - taqo (default) or regression')
    parser.add_argument('--model',
                        default="simple",
                        help='Test model to use - simple or tpch')

    parser.add_argument('--num-queries',
                        default=0,
                        help='Number of queries for default model')
    parser.add_argument('--num-retries',
                        default=5,
                        help='Number of retries')
    parser.add_argument('--skip-timeout',
                        default=2,
                        help='Timeout delta for optimized query')
    parser.add_argument('--num-optimizations',
                        default=300,
                        help='Maximum number of allowed optimizations (default 300)')

    parser.add_argument('--asciidoctor-path',
                        default="asciidoctor",
                        help='Full path to asciidoc command (default asciidoctor)')

    args = parser.parse_args()

    if args.test == "taqo":
        evaluate_taqo(args)
    elif args.test == "regression":
        evaluate_regression(args)
