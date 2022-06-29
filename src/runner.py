import argparse

from regression import evaluate_regression
from taqo import evaluate_taqo


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
                        help='Target database in YugabyteDB')

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
                        default=0,
                        help='Maximum number of allowed optimizations (default 0 - disabled)')

    args = parser.parse_args()

    if args.test == "taqo":
        evaluate_taqo(args)
    elif args.test == "regression":
        evaluate_regression(args)
