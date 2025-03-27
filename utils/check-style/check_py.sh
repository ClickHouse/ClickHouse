#!/bin/bash

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

start_total=`date +%s`

start=`date +%s`
echo "Check " | ts
./check-black -n              |& tee /test_output/black_output.txt
runtime=$((`date +%s`-start))
echo "Check python formatting with black. Done. $runtime seconds."

start=`date +%s`
echo "Check " | ts
./check-isort -n              |& tee /test_output/isort_output.txt
runtime=$((`date +%s`-start))
echo "Check python formatting with isort. Done. $runtime seconds."

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
