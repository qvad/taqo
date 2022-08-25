# Table of Contents

1. [Query Optimizer Testing Framework](#query-optimizer-testing-framework)
2. [Model](#model)
    1. [SQL model](#sql-model)
    2. [Basic SQL model](#basic-sql-model)
    3. [Other models](#other-models)
        1. [Complex model](#complex-model)
        2. [ClickBench OLAP model](#clickbench-olap-model)
3. [Tests](#tests)
    1. [TAQO-inspired test](#taqo-inspired-test)
    1. [Regression test](#regression-test)
    1. [Custom tests](#custom-tests)
4. [Setup](#setup)
    1. [Additional dependencies](#additional-dependencies)
5. [Runner and configuration](#runner-and-configuration)
    1. [Configuration (TODO)](#configuration-todo)
    2. [Runner (TODO)](#runner-todo)

# Query Optimizer Testing Framework

Idea of framework is to provide semi-automated tests that may help in testing and validating query
optimizer current performance and changes. Main goal is to provide human readable reports and
probably automate some checks.

There are 2 main essences in the framework - *Model* and *Test/Report*.

----
## Model

Model is a set of DDLs that define the model and select queries that need to be tested

### SQL model

Custom model where use can define any queries in `*.sql` files. Contains two types of
files `create.sql`
and `queries/*.sql` - create is for creating model tables and uploading data, queries is a set of
single query files that we want to measure. Example for TPCH model can be found in `sql/` directory.

```
sql/$MODEL_NAME/create.sql
sql/$MODEL_NAME/queries/*.sql
```

### Basic SQL model

This model is trying to cover most usable features in optimizer, so that on regression/comparison
test all problems should be visible. See `sql/basic/*` structure.

### Other models

#### Complex model

Generated queries that focus on testing different Joins. See `src/models/complex.py`

#### ClickBench OLAP model

Model based on ClickBench. See `sql/clickbench/**`

----
## Tests

Tests are a sequence of following actions: creating tables (if needed), running queries, collecting
results, evaluating a few automated checks and then finally generating a report in ASCIIDOC format (
Something like extended Markdown format). This specific format allow to create complex html files
with code syntax highlight inside tables eg. Framework supports adding new scenarios.

### TAQO-inspired test

This test is inspired
by [TAQO](https://www.researchgate.net/publication/241623318_Testing_the_accuracy_of_query_optimizers)
page. Idea is to evaluate query and all possible optimisations for it. After that we compare
execution time and other parameters to tell if optimiser works well. Query evaluated few times (
see `--num-retries`) to avoid invalid results.

Test detects all tables that are used in query, generates all possible permutations (basically
framework tries to generate all possible `Leading` hints) and then tries to generate possible
optimizations using pg_hint by combining current table permutation with different types of Joins (
Nested Loop Join, Merge, Hash) and scans (Index if available, Sequential).

For example for 3 tables `‘a’, ‘b’, ‘c’` there will be following permutations generated:
`[('a', 'b', 'c'), ('a', 'c', 'b'), ('b', 'a', 'c'), ('b', 'c', 'a'), ('c', 'a', 'b'), ('c', 'b', 'a')]]`
. Each permutation will be transformed into a Leading hint (`Leading ((a b) c)` `Leading ((a c) b)`
etc). After all leading hints are generated, the tool will try to generate all possible combinations
of NL, Merge, Hash joins
(`Leading ((a b) c) Merge(a b) Merge(a b c)`, `Leading ((a b) c) Merge(a b) Hash(a b c)` etc) .
After all joins are used, the tool will apply all possible combinations of scans based on the tables
used and its indexes.
(For now Index scan just applies on assumption that if there is an Index on table - it will be
applied, no matter which columns are selected). For 4 tables there will be 423 possible `pg_hint`
for example.

To reduce the number of generated optimizations, a pairwise approach may be used (enabled by
default): it guarantees that two different tables will be tried to be joined by NLJ, Merge, Hash and
both of them will be tried to be scanned by all possible Seq and Index combinations. But for 3
tables for example there is no such assumption. Another option to reduce number of optimizations is
using comment hints in `*.sql` files - comma separated accepted and rejected substrings
of `pg_hints`
can be mentioned there:

```sql
-- accept: a b c
-- reject: NestLoop
-- max_timeout: 5s

select a.c1,
       a.c2,
    ...
```

In this example framework will only use join order from the accept hint and reject all NestLoop
joins. Max query timeout will be limited by 1 second.

After optimizations are generated, the framework evaluates all of them with maximum query timeout
equal to current minimum execution time (starts with original optimization timeout) so don’t spend
time on worst cases.

### Regression test

Test evaluates same queries w/o any optimization hints against 2 different versions and generates
the report with diff analysis. Idea is to check if there are any regressions in default execution
plan generation. Ideally it must be evaluated against the same hardware so after upgrading the
cluster all data skews will be the same.

Scenario:

1. Start cluster of version1.
2. Evaluate all queries and store results for version1
3. Upgrade cluster to version2.
4. Evaluate all queries and store results for version2
5. Compare execution plans version1 vs version2
6. Generate report

### Custom tests

Any custom report and test can be implemented here - comparison with other DBs, validate specific flags etc.

----
# Setup

Install Python dependencies `pip install -r requirements.txt` and setup you SUT database.

### Additional dependencies

To generate PDF/HTML from `.adoc` file [asciidoc utility](https://asciidoc.org/) needed. In some
environments `--asciidoctor-path` must be specified, e.g. for MacOS Homebrew installation value
should be `/opt/homebrew/bin/asciidoctor`. Note that `asciidoc` and `asciidoctor` are separate
projects!

For proper syntax highlight `coderay` must be installed by `gem install coderay`

----

# Runner and configuration

This part will be reworked soon

## Configuration (TODO)

Main idea of using configuration file here is to move some least changed stuff into a separate file
Here is all possible values that can be defined:

```hocon
yugabyte_code_path = "/yugabyte-db" # optional if local code test run
num_nodes = 1 # optional if local code or archive test run

random_seed = 2022
num_queries = -1 # limit number of queries in model 
num_retries = 5 # number of query retries

asciidoctor_path = "asciidoctor" # path to asciidoctor, can be different in brew

postgres = {
  host = "127.0.0.1"
  port = 5432
  username = "postgres"
  password = "postgres"
  database = "postgres"
}

skip_percentage_delta = 0.05 # allowed diff between queries (all tests included)

# All following parameters are related to TAQO only
use_allpairs = true # reduce number of generated optimizations
max_optimizations = 1000 # limit maximum number of optimizations 
report_near_queries = true # report best optimization plans
skip_table_scans_hints = false # use only join hints
skip_timeout_delta = 1 # skip queries if they exceed (min+1) seconds
look_near_best_plan = true # evaluate only queries that are near current best optimization

```

## Runner (TODO)

```
usage: runner.py [-h] [--config CONFIG] [--test TEST] [--model MODEL]
                 [--previous_results_path PREVIOUS_RESULTS_PATH] [--basic_multiplier BASIC_MULTIPLIER]
                 [--yugabyte_code_path YUGABYTE_CODE_PATH] [--revisions REVISIONS] 
                 [--num_nodes NUM_NODES] [--tserver_flags TSERVER_FLAGS] [--master_flags MASTER_FLAGS]       
                 [--host HOST] [--port PORT] [--username USERNAME] [--password PASSWORD] 
                 [--database DATABASE] [--enable_statistics | --no-enable_statistics]
                 [--num_queries NUM_QUERIES] [--compare_with_pg | --no-compare_with_pg] 
                 [--skip_model_creation | --no-skip_model_creation] [--clear | --no-clear]
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

### Example of launch command

```sh
python3 src/runner.py --model=basic --test=regression --revisions=dc811063,a71c9 --config=config/mine.conf --enable_statistics
```




