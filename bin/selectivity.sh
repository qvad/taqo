#!/bin/bash

while getopts ":c:m:r:" opt; do
  case $opt in
  c)
    config="$OPTARG"
    ;;
  m)
    model="$OPTARG"
    ;;
  r)
    rev="$OPTARG"
    ;;
  \?)
    echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac

  case $OPTARG in
  -*)
    echo "Option $opt needs a valid argument"
    exit 1
    ;;
  esac
done

echo "Evaluating default test against $rev"
python3 src/runner.py collect --no-clean-db --model=$model --config=$config --revision=$rev --output=d_$model$rev --ddls=database,create,drop,import --explain-clause="explain" --yes
echo "Evaluating default test against $rev with table analyze"
python3 src/runner.py collect --model=$model --config=$config --output=da_$model$rev --ddls=none --explain-clause="explain analyze" --yes

echo "Evaluating table stats test against $rev"
python3 src/runner.py collect --model=$model --config=$config --output=ta_$model$rev --ddls=analyze --explain-clause="explain" --yes
echo "Evaluating table stats test against $rev with table analyze"
python3 src/runner.py collect --model=$model --config=$config --output=taa_$model$rev --ddls=none --explain-clause="explain analyze" --yes

echo "Evaluating CBO test against $rev"
python3 src/runner.py collect --model=$model --config=$config --output=tsa_$model$rev --ddls=none --session-props="SET yb_enable_optimizer_statistics = true;" --explain-clause="explain" --yes
echo "Evaluating CBO test against $rev with table analyze"
python3 src/runner.py collect --model=$model --config=$config --output=tsaa_$model$rev --ddls=none --explain-clause="explain analyze" --session-props="SET yb_enable_optimizer_statistics = true;" --yes

echo "Generating report"
python3 src/runner.py report --type=selectivity --config=$config \
  --default-results=report/d_$model$rev.json --default-analyze-results=report/da_$model$rev.json \
  --ta-results=report/ta_$model$rev.json --ta-analyze-results=report/taa_$model$rev.json \
  --stats-results=report/tsa_$model$rev.json --stats-analyze-results=report/tsaa_$model$rev.json
