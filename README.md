# Query Optimizer Testing Framework

The idea of the framework is to automate routines around query optimizer testing and generate a
human-readable report that can be manually verified. Since the query optimizer depends on data in
the cluster, hardware, etc., all test scenarios are designed to reduce these effects and exclude
hard checks in fail criteria.

# Installation

There are few things required before TAQO can be fully usable.

1. Install python3.10+
2. Create `venv` if needed
3. Install python requirements `pip install -r requirements.txt`

### Additional dependencies

To generate PDF/HTML from `.adoc` file [asciidoc utility](https://asciidoc.org/) needed. In some
environments `--asciidoctor-path` must be specified, e.g. for macOS Homebrew installation value
should be `/opt/homebrew/bin/asciidoctor`. Note that `asciidoc` and `asciidoctor` are separate
projects!

For proper syntax highlight `coderay` must be installed by `gem install coderay`

----

There are 3 main essences in the framework - **Model** and actions: **Collect** and **Report**.

## Model

Model is a set of DDLs that define the model and select queries that need to be tested

### SQL model

Custom model where use can define any queries in `*.sql` files. Contains two types of
files `create.sql`
and `queries/*.sql` - create is for creating model tables and uploading data, queries is a set of
single query files that we want to measure. Example for TPCH model can be found in `sql/` directory.

---
**NOTE**

Due to SQL parsing limitations, a query can either have all tables with aliases or no aliases should
be used.

---

```
sql/$MODEL_NAME/drop.sql
sql/$MODEL_NAME/create.sql
sql/$MODEL_NAME/import.sql
sql/$MODEL_NAME/analyze.sql
sql/$MODEL_NAME/postgres.create.sql
sql/$MODEL_NAME/obsolete.create.sql
sql/$MODEL_NAME/queries/*.sql
```

Use the sql/proprietary/ folder for any models you don't want to be checked in. Remember to prefix
the model flag with proprietary/ in this case.

#### DDL Prefix

The `--ddl-prefix` flag feature has been implemented to support various scenarios that rely on DDL
calls. This feature allows specific DDL queries to be executed to test different features. For
example, the default Yugabyte implementation assumes that the database will be created with a
colocation flag. However, Postgres does not have this feature. To evaluate tests against Postgres,
you need to define `--ddl-prefix=postgres`. Additionally, if `--db=postgres`, then `--ddl-prefix`
will be automatically defined based on the database type.

This feature can also be used to test obsolete configurations. For example, at Yugabyte, the old
syntax `colocated` was used, which can be tested using the `--ddl-prefix=colocated` flag.

#### Prefix semantics

There are five DDL stages - database, drop, create, import, and analyze. Each step, except for the
database stage, is mapped to a corresponding file name: create -> create.sql, import -> import.sql,
etc. If --ddl-prefix is defined, the framework will try to search for $PREFIX.create.sql file for
the create DDL stage. If the file is not found, create.sql will be used as the default one.

### basic

This model is trying to cover most usable features in optimizer, so that on regression/comparison
test all problems should be visible. See `sql/basic/*` structure.

### complex

Generated queries that focus on testing different joins and sub-queries

### join-order-benchmark

See [join-order-benchmark](https://github.com/gregrahn/join-order-benchmark)

----

## Actions

Basic evaluation contains two actions - collect and report. Collect will evaluate all queries in the
model and store it into a JSON file. On report action users need to define one or few JSON files and
based on that different reports will be generated.
Collect
Test is a sequence of following actions: evaluate DDLs (if needed), running queries, evaluate
possible optimizations (if needed) and finally collect results to the JSON file.

In all tests the framework is able to detect the equal execution plans, the main validation criteria
is query execution time. For regression tests RPC Calls, Memory Usage, Rows scanned metrics from
EXPLAIN ANALYZE are also available.
Each query evaluated few times (6 times total by default, first execution stats are skipped, the
final execution time will be AVG from later 5 tries)

### Collecting optimizations (Based on TAQO paper)

This algorithm is inspired
by [TAQO](https://www.researchgate.net/publication/241623318_Testing_the_accuracy_of_query_optimizers)
page. Idea is to evaluate query and all possible optimisations for it. After that we compare
execution time and other parameters to tell if optimiser works well. Query evaluated few times (
see `--num-retries`) to avoid inaccurate results.

Tool detects all tables that are used in query, generates all possible permutations (basically
framework tries to generate all possible `Leading` hints) and then tries to generate possible
optimizations using pg_hint by combining current table permutation with different types of Joins (
Nested Loop Join, Merge, Hash) and scans (Index if available, Sequential).

For example for 3 tables `‘a’, ‘b’, ‘c’` there will be following permutations generated:
`[('a', 'b', 'c'), ('a', 'c', 'b'), ('b', 'a', 'c'), ('b', 'c', 'a'), ('c', 'a', 'b'), ('c', 'b', 'a')]]`
. Each permutation will be transformed into a Leading hint (`Leading ((a b) c)` `Leading ((a c) b)`
etc.). After all leading hints are generated, the tool will try to generate all possible
combinations
of NL, Merge, Hash joins
(`Leading ((a b) c) Merge(a b) Merge(a b c)`, `Leading ((a b) c) Merge(a b) Hash(a b c)` etc) .
After all joins are used, the tool will apply all possible combinations of scans based on the tables
used and its indexes.
(For now Index scan just applies on assumption that if there is an Index on table - it will be
applied, no matter which columns are selected). For 4 tables there will be 423 possible `pg_hint`
for example.

To reduce the number of generated optimizations, a pairwise approach is used (enabled by default if
there is more than 4 tables in query): it guarantees that 3 different tables will be tried to be
joined by NLJ, Merge, Hash combinations and all of them will be tried to be scanned by all possible
Seq and Index combinations. But for 4 tables for example there is no such assumption.

Here is an example how pairwise will reduce the number of join combinations: suppose we have 4
tables t1, t2, t3 and t4 in the query. There are 3 joins that should appear in the execution plan,
one of `['Nested','Hash','Merge']` for each 2 tables. Here is the list of combinations that
generated
by using pairwise approach: `[['Nested', 'Nested', 'Nested'], ['Hash', 'Hash', 'Nested']
, ['Merge', 'Merge', 'Nested'], ['Merge', 'Hash', 'Hash'], ['Hash', 'Nested', 'Hash']
, ['Nested', 'Merge', 'Hash'], ['Nested', 'Hash', 'Merge'], ['Hash', 'Merge', 'Merge']
, ['Merge', 'Nested', 'Merge']]`. Note that here each 3 tables (2 joins) will be tried to be joined
by each join type, but for example `[‘Merge’,'Merge','Merge']` combination is not here.

There is `all-pairs-threshold` parameter in configuration - it defines maximum number of tables in
query after which
pairwise approach will be used. By default, this threshold is equal to `3`. For this value, for
example,`basic` model will
be evaluated without using pairwise, while JOB and complex models will reduce number of
combinations.

Another option to
reduce number of optimizations is using comment hints in `*.sql` files - comma separated accepted
and rejected
substrings of `pg_hints`can be mentioned there:

```sql
-- accept: a b c
-- reject: NestLoop
-- max_timeout: 5s
-- tags: muted_nlj, 5s_max
-- debug_hints: set (yb_enable_optimizer_statistics false)

select a.c1,
       a.c2, ...
```

In this example framework will only use join order from the accept hint and reject all NestLoop
joins. Max query timeout will be limited by 5 seconds.

After optimizations are generated, the framework evaluates all of them with maximum query timeout
equal to current minimum execution time (starts with original optimization timeout) so do not spend
time on worst cases.

----

## Report

Report action evaluates a few automated checks based on provided data and then generates reports in
ASCIIDOC format (Something like extended Markdown format). This specific format allow to create
complex html files with code syntax highlight inside tables e.g. Framework supports adding new
scenarios.

### TAQO/Score

TAQO report is a basic report that analyzes QO performance. For this test user need to provide a
JSON file with optimizations. Based on this information report will show TAQO plot, score, the best
optimization and how it differs with default one. In addition, user can provide PG results, in this
case there will be also comparison with PG execution plans if specified.

### Default execution plan comparison

These reports do not require optimizations to be evaluated, to test itself might be quick.

#### Regression and Comparison

See `bin/regression.sh` for steps.

1. Start cluster of version1.
2. Evaluate all queries and store results for version1
3. Upgrade cluster to version2 (or against PG compatible DB).
4. Evaluate all queries and store results for version2
5. Generate `report` with plans comparison version1 vs version2

#### Selectivity testing

See `bin/selectivity.sh` for steps.

1. Evaluate EXPLAIN query
2. Evaluate EXPLAIN ANALYZE query
3. Run ANALYZE on all tables
4. Evaluate EXPLAIN query
5. Evaluate EXPLAIN ANALYZE query
6. Enable statistics hint
7. Evaluate EXPLAIN query
8. Evaluate EXPLAIN ANALYZE query
9. Generate `report` with plans comparison between 6 different result files

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
explain-clause = "explain "
# session properties before executing set of testing queries
session-props = [
  "SET pg_hint_plan.enable_hint = ON;",
  "SET pg_hint_plan.debug_print = ON;",
  "SET client_min_messages TO log;",
  "SET pg_hint_plan.message_level = debug;",
]

# allowed diff between queries (all tests included)
skip-percentage-delta = 0.15

# query execution related options
ddl-query-timeout = 3600 # skip DDL if they evaluated in more than 3600 seconds
test-query-timeout = 1200 # skip queries if they evaluated in more than 1200 seconds

# optimization generation
skip-timeout-delta = 1 # skip queries if they exceed (min+1) seconds
all-pairs-threshold = 3 # maximum number of tables after which all_pairs will be used, -1 to use all combinations always
look-near-best-plan = true # evaluate only queries that are near current best optimization

# limit number of queries in model, needed for debug
num-queries = -1
# number of retries to get query execution time
num-retries = 5
num-warmup = 1

# path to asciidoctor, can be different in brew
asciidoctor-path = "asciidoctor"
```

## Runner

Here is full description of all available arguments.

```
Query Optimizer Testing framework for PostgreSQL compatible DBs

positional arguments:
  action                Action to perform - collect or report

options:
  -h, --help            show this help message and exit
  --db DB               Database to run against
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
  --remote-data-path REMOTE_DATA_PATH
                        Path to remote data files ($DATA_PATH/*.csv)
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
python3 src/runner.py
collect
--optimizations
--model=basic
--config=config/default.conf
--output=taqo_basic_yb
--database=taqo
```

Generate comparison report for 2 previous collect runs

```
python3 src/runner.py
report
--type=regression
--config=config/qo.conf
--v1-results=report/basic_taqo_new_runner.json
--v2-results=report/basic_taqo_new_runner_2.json
```

Generate score report which contains taqo analysis and comparison with postgres

```
python3 src/runner.py
report
--type=score
--config=config/qo.conf
--results=report/basic_taqo_yb.json
--pg-results=report/basic_taqo_pg.json
```
