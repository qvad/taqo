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
usage: runner.py [-h] [--config CONFIG] [--test TEST] [--model MODEL] [--previous_results_path PREVIOUS_RESULTS_PATH] [--basic_multiplier BASIC_MULTIPLIER]
                 [--yugabyte_code_path YUGABYTE_CODE_PATH] [--revisions REVISIONS] [--num_nodes NUM_NODES] [--tserver_flags TSERVER_FLAGS] [--master_flags MASTER_FLAGS]       
                 [--host HOST] [--port PORT] [--username USERNAME] [--password PASSWORD] [--database DATABASE] [--enable_statistics | --no-enable_statistics]
                 [--num_queries NUM_QUERIES] [--compare_with_pg | --no-compare_with_pg] [--skip_model_creation | --no-skip_model_creation] [--clear | --no-clear]
                 [--verbose | --no-verbose]

Query Optimizer Testing framework for PostgreSQL compatible DBs

options:
  -h, --help            show this help message and exit
  --config CONFIG       Configuration file path
  --test TEST           Type of test to evaluate - taqo (default) or regression
  --model MODEL         Test model to use - complex, tpch, subqueries, any other custom model
  --previous_results_path PREVIOUS_RESULTS_PATH
                        Path to previous execution results. May be used in regression and comparison reports
  --basic_multiplier BASIC_MULTIPLIER
                        Basic model data multiplier (Default 10)
  --yugabyte_code_path YUGABYTE_CODE_PATH
                        Code path to yugabyte-db repository
  --revisions REVISIONS
                        Comma separated git revisions or paths to release builds
  --num_nodes NUM_NODES
                        Number of nodes
  --tserver_flags TSERVER_FLAGS
                        Comma separated tserver flags
  --master_flags MASTER_FLAGS
                        Comma separated master flags
  --host HOST           Target host IP for postgres compatible database
  --port PORT           Target port for postgres compatible database
  --username USERNAME   Username for connection
  --password PASSWORD   Password for user for connection
  --database DATABASE   Target database in postgres compatible database
  --enable_statistics, --no-enable_statistics
                        Evaluate yb_enable_optimizer_statistics before running queries (default: False)
  --num_queries NUM_QUERIES
                        Number of queries to evaluate
  --compare_with_pg, --no-compare_with_pg
                        Add compare with postgres to report (default: False)
  --skip_model_creation, --no-skip_model_creation
                        Skip model creation queries (default: False)
  --clear, --no-clear   Clear logs directory (default: False)
  --verbose, --no-verbose
                        Enable DEBUG logging (default: False)

```

## Models

### SQL model

Custom model where use can define any `*sql` queries. Contains two types of files - `create.sql` and `queries/*.sql` -
create is for creating model tables and uploading data,  queries is a set of single query files that we want to measure.
Example for TPCH model can be found in `sql/` directory.
```
sql/$MODEL_NAME/create.sql
sql/$MODEL_NAME/queries/*.sql
```

##### Query tips example

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

#### Basic model

This model is trying to cover most usable features in optimizer, so that on regression/comparison test all problems should be visible.


### Other models

#### Complex model

Generated queries that focus on testing different Joins.

#### ClickBench OLAP model

Model based on ClickBench.

## Tests

### TAQO-inspired test

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


