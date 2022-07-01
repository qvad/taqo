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
                 [--password PASSWORD] [--database DATABASE]
                 [--enable-statistics | --no-enable-statistics]
                 [--explain-analyze | --no-explain-analyze] [--test TEST]
                 [--model MODEL]
                 [--skip-model-creation | --no-skip-model-creation]
                 [--num-queries NUM_QUERIES] [--num-retries NUM_RETRIES]
                 [--skip-timeout-delta SKIP_TIMEOUT_DELTA]
                 [--max-optimizations MAX_OPTIMIZATIONS]
                 [--asciidoctor-path ASCIIDOCTOR_PATH]
                 [--verbose | --no-verbose]

Query Optimizer Testing framework for PostgreSQL compatible DBs

options:
  -h, --help            show this help message and exit
  --host HOST           Target host IP for postgres compatible database
  --port PORT           Target port for postgres compatible database
  --username USERNAME   Username for connection
  --password PASSWORD   Password for user for connection
  --database DATABASE   Target database in postgres compatible database
  --enable-statistics, --no-enable-statistics
                        Evaluate yb_enable_optimizer_statistics before running
                        queries (default: False)
  --explain-analyze, --no-explain-analyze
                        Evaluate EXPLAIN ANALYZE instead of EXPLAIN (default:
                        False)
  --test TEST           Type of test to evaluate - taqo (default) or
                        regression
  --model MODEL         Test model to use - simple (default) or tpch
  --skip-model-creation, --no-skip-model-creation
                        Skip model creation queries (default: False)
  --num-queries NUM_QUERIES
                        Number of queries for default model
  --num-retries NUM_RETRIES
                        Number of retries
  --skip-timeout-delta SKIP_TIMEOUT_DELTA
                        Timeout delta for optimized query
  --max-optimizations MAX_OPTIMIZATIONS
                        Maximum number of allowed optimizations (default 300)
  --asciidoctor-path ASCIIDOCTOR_PATH
                        Full path to asciidoc command (default asciidoctor)
  --verbose, --no-verbose
                        Enable extra logging (default: False)
```

## Models

### Simple model

Simple model is a set of generated queries with all possible join types. No complex queries or conditions here.

### SQL model

Custom model where use can define any `*sql` queries. Contains two types of files - `create.sql` and `queries/*.sql` - 
create is for creating model tables and uploading data,  queries is a set of single query files that we want to measure.
Example for TPCH model can be found in `sql/` directory.
```
sql/$MODEL_NAME/create.sql
sql/$MODEL_NAME/queries/*.sql
```

#### Query tips example

For each query following tips can be added as a comment in top of query file. Tips should be comma separated and possible used in hints.

In following example we accept query if `part supplier nation` mentioned, reject if any of `nation region` or `NestLoop` used and 
defining `max_timeout` to `1s`.
```sql
-- accept: part supplier nation
-- reject: nation region, NestLoop
-- max_timeout: 1s
```

While constructing possible optimizations code will `accept` all optimizations what contains accept fields, `reject` for opposite reasons,
and additional `max_timeout` tip that can limit maximum optimization execution time. Note that `max_timeout` value will be directly used in 
`SET statement_timeout = 'MAX_TIMEOUT'` statement, so it MUST include type of value (`s` for seconds and so on)

## Tests

### Run TAQO-inspired test

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

### Run regression test

NOT FULLY IMPLEMENTED

Regression test will evalute same model for 2 different version - firstly it evaluate same queries for one version.
After that DB must be upgraded to a newer version to keep all data skews same.
Finally we evaluate queries for second version.


