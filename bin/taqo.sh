#!/bin/bash

while getopts ":c:m:r:" opt; do
  case $opt in
    c) config="$OPTARG"
    ;;
    m) model="$OPTARG"
    ;;
    r) rev="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac

  case $OPTARG in
    -*) echo "Option $opt needs a valid argument"
    exit 1
    ;;
  esac
done

echo "Evaluating test against YB $rev"
python3 src/runner.py collect --model=$model --config=$config --revision=$rev --output=yb_$model$rev --optimizations

echo "Evaluating test against PG $rev (localhost:5432)"
python3 src/runner.py collect --model=$model --config=$config --revision=$rev --output=pg_$model$rev --optimizations --port=5432

echo "Generating TAQO report"
python3 src/runner.py report --type=taqo --config=$config --results=report/yb_$model$rev.json --pg_results=report/pg_$model$rev.json