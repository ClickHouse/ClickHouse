#!/bin/bash

# yaml check is not the best one

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

start_total=`date +%s`

start=`date +%s`
./check-typos                 |& tee /test_output/typos_output.txt
runtime=$((`date +%s`-start))
echo "Check typos. Done. $runtime seconds."

start=`date +%s`
./check-doc-aspell            |& tee /test_output/docs_spelling_output.txt
runtime=$((`date +%s`-start))
echo "Check docs spelling. Done. $runtime seconds."

runtime=$((`date +%s`-start_total))
echo "Check Docs, total. Done. $runtime seconds."
