#!/bin/bash

# yaml check is not the best one

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

start_total=`date +%s`

# FIXME: 30 sec to wait
# echo "Check duplicates" | ts
# ./check-duplicate-includes.sh |& tee /test_output/duplicate_includes_output.txt

start=`date +%s`
./check-style -n              |& tee /test_output/style_output.txt
runtime=$((`date +%s`-start))
echo "Check style. Done. $runtime seconds."

start=`date +%s`
./check-whitespaces -n        |& tee /test_output/whitespaces_output.txt
runtime=$((`date +%s`-start))
echo "Check whitespaces. Done. $runtime seconds."

start=`date +%s`
./check-workflows             |& tee /test_output/workflows_output.txt
runtime=$((`date +%s`-start))
echo "Check workflows. Done. $runtime seconds."

start=`date +%s`
./check-submodules            |& tee /test_output/submodules_output.txt
runtime=$((`date +%s`-start))
echo "Check submodules. Done. $runtime seconds."

# FIXME: 6 min to wait
# echo "Check shell scripts with shellcheck" | ts
# ./shellcheck-run.sh           |& tee /test_output/shellcheck_output.txt

runtime=$((`date +%s`-start_total))
echo "Check style total. Done. $runtime seconds."
