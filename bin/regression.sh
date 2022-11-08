#!/bin/bash

while getopts ":c:m:f:s:" opt; do
  case $opt in
    c) config="$OPTARG"
    ;;
    m) model="$OPTARG"
    ;;
    f) rev_1="$OPTARG"
    ;;
    s) rev_2="$OPTARG"
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

echo "Evaluating test against $rev_1"
python3 src/runner.py collect --model=$model --config=$config --revision=$rev_1 --output=reg_$model$rev_1 --yes

echo "Evaluating test against $rev_2"
python3 src/runner.py collect --model=$model --config=$config --revision=$rev_1 --output=reg_$model$rev_2 --yes

echo "Generating report"
python3 src/runner.py report --type=regression --config=$config --v1-results=report/reg_$model$rev_1.json --v2-results=report/reg_$model$rev_2.json