# Table of Contents

1. [Query Optimizer Testing Framework](#query-optimizer-testing-framework)
2. [Installation](#installation)
3. [Model](#model)
    1. [SQL model](#sql-model)
    2. [Basic SQL model](#basic-sql-model)
    3. [Other models](#other-models)
        1. [Complex model](#complex-model)
        2. [ClickBench OLAP model](#clickbench-olap-model)
4. [Tests(outdated)](#tests)
    1. [TAQO-inspired test](#taqo-inspired-test)
    1. [Regression test](#regression-test)
    1. [Custom tests](#custom-tests)
5. [Setup](#setup)
    1. [Additional dependencies](#additional-dependencies)
6. [Runner and configuration](#runner-and-configuration)
    1. [Configuration](#configuration-todo)
    2. [Runner](#runner-todo)
7. [Launch command examples](#launch-command-examples)

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

#### Subqueries

Small set of queries with subqueries and joins

#### Complex model

Generated queries that focus on testing different Joins. See `src/models/complex.py`

#### ClickBench queries

Model based on [ClickBench](https://github.com/ClickHouse/ClickBench). See `sql/clickbench/**`

#### TPCH

Popular benchmark to test OLAP DBs

#### join-order-benchmark queries

See [join-order-benchmark](https://github.com/gregrahn/join-order-benchmark)

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

For local execution and some Jenkins cases framework can use `yugabyte-db` repo to start a cluster automatically.
There is also a feature that allow to separate regression test run into 2 different runs - for version1 and then run again for version2, in this case scenario will be following:

1. Start cluster of version1.
2. Evaluate all queries and store results for version1 into a file
3. Execution of framework will be stopped here
4. Upgrade cluster to version2 or run with other connections params
5. Start testing framework again
6. Evaluate all queries and store results for version2
7. Compare execution plans version1 vs version2 (version1 results will be taken from step 2)
8. Generate report


### Comparison with PG

Same as the regression test, but compare execution plans with Postgres (or any other PG compatible DB) and use specific fail criteria (Execution time should be ~3x PG performance e.g.).
Result is a report with a table with following layout:

| DB execution time | Postgres execution time  | Ratio vs Postgres | Ratio vs Postgres x3 |
|-------------------|---|---|----------------------|

1. Connect to DB cluster.
2. Evaluate all queries from Model and store results
3. Connect to Postgres DB.
4. Evaluate all queries from Model and store results
5. Compare execution plans and generate the report. All failed ratios will be marked as red.

### Selectivity testing

1. Evaluate EXPLAIN query
2. Evaluate EXPLAIN ANALYZE query
3. Run ANALYZE on all tables
4. Evaluate EXPLAIN query
5. Evaluate EXPLAIN ANALYZE query
6. Enable statistics hint
7. Evaluate EXPLAIN query
8. Evaluate EXPLAIN ANALYZE query

----
# Setup

There are few things required before TAQO can be fully usable.

1. Install python3.10+
2. Install python requirements `python3.10 -m pip install -r requirements.txt`
3. Install `asciidoctor` (NOT asciidoc!) and coderay `gem install coderay`

### Additional dependencies

To generate PDF/HTML from `.adoc` file [asciidoc utility](https://asciidoc.org/) needed. In some
environments `--asciidoctor-path` must be specified, e.g. for MacOS Homebrew installation value
should be `/opt/homebrew/bin/asciidoctor`. Note that `asciidoc` and `asciidoctor` are separate
projects!

For proper syntax highlight `coderay` must be installed by `gem install coderay`

----

# Runner and configuration

## Configuration

Main idea of using configuration file here is to move some least changed stuff into a separate file
Here is all possible values that can be defined:

```hocon
# optional if local code test run
source-path = "/yugabyte-db"
# optional if local code or archive test run
num-nodes = 3

# default explain clause
# will be used in TAQO, regression, comparison tests as a plan extraction command
explain-clause = "explain (analyze)"
# session properties before executing set of testing queries
session-props = [
   "SET pg_hint_plan.enable_hint = ON;",
   "SET pg_hint_plan.debug_print = ON;",
   "SET client_min_messages TO log;",
   "SET pg_hint_plan.message_level = debug;",
]

# default random seed, used in all tests
random-seed = 2022
# allowed diff between queries (all tests included)
skip_percentage-delta = 0.25

# TAQO related options
skip-timeout-delta = 1 # skip queries if they exceed (min+1) seconds
all-pairs-threshold = 3 # maximum number of tables after which all_pairs will be used, -1 to use all combinations always
look-near-best-plan = true # evaluate only queries that are near current best optimization

# limit number of queries in model, needed for debug
num-queries = -1
# number of retries to get query execution time
num-retries = 5

# path to asciidoctor, can be different in brew
asciidoctor-path = "asciidoctor"
```

## Runner

```
usage: runner.py [-h] [--config CONFIG] [--type TYPE] [--results RESULTS] [--pg-results PG_RESULTS] [--v1-results V1_RESULTS] [--v2-results V2_RESULTS] [--default-results DEFAULT_RESULTS]
                 [--default-analyze-results DEFAULT_ANALYZE_RESULTS] [--ta-results TA_RESULTS] [--ta-analyze-results TA_ANALYZE_RESULTS] [--stats-results STATS_RESULTS]
                 [--stats-analyze-results STATS_ANALYZE_RESULTS] [--ddl-prefix DDL_PREFIX] [--optimizations | --no-optimizations] [--model MODEL] [--basic-multiplier BASIC_MULTIPLIER]
                 [--source-path SOURCE_PATH] [--revision REVISION] [--ddls DDLS] [--clean-db | --no-clean-db] [--allow-destroy-db | --no-allow-destroy-db] [--clean-build | --no-clean-build]
                 [--num-nodes NUM_NODES] [--tserver-flags TSERVER_FLAGS] [--master-flags MASTER_FLAGS] [--host HOST] [--port PORT] [--username USERNAME] [--password PASSWORD] [--database DATABASE]
                 [--enable-statistics | --no-enable-statistics] [--explain-clause EXPLAIN_CLAUSE] [--num-queries NUM_QUERIES] [--parametrized | --no-parametrized] [--output OUTPUT] [--clear | --no-clear]
                 [--yes | --no-yes] [--verbose | --no-verbose]
                 action

Query Optimizer Testing framework for PostgreSQL compatible DBs

positional arguments:
  action                Action to perform - collect or report

options:
  -h, --help            show this help message and exit
  --config CONFIG       Configuration file path
  --type TYPE           Report type - taqo, regression, comparison or selectivity
  --results RESULTS     TAQO/Comparison: Path to results with optimizations for YB
  --pg-results PG_RESULTS
                        TAQO/Comparison: Path to results for PG, optimizations are optional
  --v1-results V1_RESULTS
                        Regression: Results for first version
  --v2-results V2_RESULTS
                        Regression: Results for second version
  --default-results DEFAULT_RESULTS
                        Results for no optimizer tuned DB
  --default-analyze-results DEFAULT_ANALYZE_RESULTS
                        Results for no optimizer tuned DB with EXPLAIN ANALYZE
  --ta-results TA_RESULTS
                        Results with table analyze
  --ta-analyze-results TA_ANALYZE_RESULTS
                        Results with table analyze with EXPLAIN ANALYZE
  --stats-results STATS_RESULTS
                        Results with table analyze and enabled statistics
  --stats-analyze-results STATS_ANALYZE_RESULTS
                        Results with table analyze and enabled statistics and EXPLAIN ANALYZE
  --ddl-prefix DDL_PREFIX
                        DDL file prefix (default empty, might be postgres)
  --optimizations, --no-optimizations
                        Evaluate optimizations for each query (default: False)
  --model MODEL         Test model to use - complex, tpch, subqueries, any other custom model
  --basic-multiplier BASIC_MULTIPLIER
                        Basic model data multiplier (Default 10)
  --source-path SOURCE_PATH
                        Path to yugabyte-db source code
  --revision REVISION   Git revision or path to release build
  --ddls DDLS           Model creation queries, comma separated: database,create,analyze,import,drop
  --clean-db, --no-clean-db
                        Keep database after test (default: True)
  --allow-destroy-db, --no-allow-destroy-db
                        Allow to run yb-ctl/yugabyted destory (default: True)
  --clean-build, --no-clean-build
                        Build yb_build with --clean-force flag (default: True)
  --num-nodes NUM_NODES
                        Number of nodes
  --tserver-flags TSERVER_FLAGS
                        Comma separated tserver flags
  --master-flags MASTER_FLAGS
                        Comma separated master flags
  --host HOST           Target host IP for postgres compatible database
  --port PORT           Target port for postgres compatible database
  --username USERNAME   Username for connection
  --password PASSWORD   Password for user for connection
  --database DATABASE   Target database in postgres compatible database
  --enable-statistics, --no-enable-statistics
                        Evaluate yb_enable_optimizer_statistics before running queries (default: False)
  --explain-clause EXPLAIN_CLAUSE
                        Explain clause that will be placed before query. Default "EXPLAIN"
  --num-queries NUM_QUERIES
                        Number of queries to evaluate
  --parametrized, --no-parametrized
                        Run parametrized query instead of normal (default: False)
  --output OUTPUT       Output JSON file name in report folder, [.json] will be added
  --clear, --no-clear   Clear logs directory (default: False)
  --yes, --no-yes       Confirm test start (default: False)
  --verbose, --no-verbose
                        Enable DEBUG logging (default: False)
```

# Launch command examples

See prepared scenarios in `bin/` directory

Collect queries results for basic model for localhost cluster
```
collect
--optimizations
--model=basic
--config=config/qo.conf
--output=taqo_complex_yb
--database=taqo
```

Generate comparison report for 2 previous collect runs
```
src/runner.py
report
--type=comparison
--config=config/dmitry.conf
--results=report/basic_taqo_new_runner.json
--pg_results=report/basic_taqo_new_runner_2.json
```