#!/bin/bash

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

start_total=$(date +%s)

start=$(date +%s)
./shellcheck-run.sh |& tee /test_output/shellcheck_output.txt
runtime=$(($(date +%s)-start))
echo "Check shellcheck. Done. $runtime seconds."

runtime=$(($(date +%s)-start_total))
echo "Check style total. Done. $runtime seconds."
