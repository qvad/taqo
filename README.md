# Query Optimizer Testing Framework

# Setup

Install Python dependencies `pip install -r requirements.txt` and setup you SUT database.

### Additional dependencies
To generate PDF/HTML from `.adoc` file [asciidoc utility](https://asciidoc.org/) needed.
In some environments `--asciidoctor-path` must be specified, 
e.g. for MacOS Homebrew installation value should be `/opt/homebrew/bin/asciidoctor`.
Note that `asciidoc` and `asciidoctor` are separate projects!

For proper syntax highlight `coderay` must be installed by `gem install coderay`

# How to run

```
usage: runner.py [-h] [--host HOST] [--port PORT] [--username USERNAME] 
                 [--password PASSWORD] [--database DATABASE] [--test TEST]
                 [--num-queries NUM_QUERIES] [--num-retries NUM_RETRIES] 
                 [--skip-timeout SKIP_TIMEOUT] [--num-optimizations NUM_OPTIMIZATIONS]
                 [--asciidoc-path ASCIIDOC_PATH]

Query Optimizer Testing framework for Posgtres-compatible DBs.

options:
  -h, --help            show this help message and exit
  --host HOST           Target host IP for YugabyteDB
  --port PORT           Target port for YugabyteDB
  --username USERNAME   Username for connection
  --password PASSWORD   Password for user for connection
  --database DATABASE   Target database in YugabyteDB
  --test TEST           Type of test to evaluate - taqo (default) or regression
  --num-queries NUM_QUERIES
                        Number of queries for default model
  --num-retries NUM_RETRIES
                        Number of retries
  --skip-timeout SKIP_TIMEOUT
                        Timeout delta for optimized query
  --num-optimizations NUM_OPTIMIZATIONS
                        Maximum number of allowed optimizations (default 0 - disabled)
  --asciidoctor-path ASCIIDOC_PATH
                        Full path to asciidoc command (default asciidoctor)

```

## Run TAQO-inspired test

This test is inspired by [TAQO](https://www.researchgate.net/publication/241623318_Testing_the_accuracy_of_query_optimizers) page.
Idea is to evaluate query and all possible optimisations for it. After that we compare execution time and other 
parameters to tell if optimiser works well. Query evaluated few times (see `--num-retries`) to avoid invalid results.

How algorithm works:

1. Create tables if needed
2. Define model - in default we use automatically generated queries with different join types. Custom SQL support will be implemented later.
3. Evaluate test for all queries
   1. Evaluate original query - EXPLAIN and query itself
   2. Based on tables that used in query generate all possible optimizations.
   3. Evaluate each optimisation and collect results
   4. Calculate TAQO score
4. Create asciidoc report

```sh
python3 src/runner.py --host localhost --test taqo --num-queries 5
```

Example output
```
Evaluating query SELECT * FROM t500000  inner join t10000... [1/5]
Setting query timeout to 3 seconds
100%|██████████| 54/54 [07:48<00:00,  8.67s/it]
Evaluating query SELECT * FROM t500000  right outer join ... [2/5]
Setting query timeout to 3 seconds
 72%|███████▏  | 39/54 [05:31<02:18,  9.25s/it]
```

## Run regression test

TODO - idea is to evaluate set of queries for 2 different versions and compare default execution plans.
