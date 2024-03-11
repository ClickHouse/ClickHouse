#!/bin/bash

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

start_total=`date +%s`

# FIXME: 1 min to wait + head checkout
echo "Check python formatting with black" | ts
./check-black -n              |& tee /test_output/black_output.txt

start=`date +%s`
./check-pylint -n               |& tee /test_output/pylint_output.txt
runtime=$((`date +%s`-start))
echo "Check pylint. Done. $runtime seconds."

start=`date +%s`
./check-mypy -n               |& tee /test_output/mypy_output.txt
runtime=$((`date +%s`-start))
echo "Check python type hinting with mypy. Done. $runtime seconds."

runtime=$((`date +%s`-start_total))
echo "Check python total. Done. $runtime seconds."
