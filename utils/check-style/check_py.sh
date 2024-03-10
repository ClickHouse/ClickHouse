#!/bin/bash

# yaml check is not the best one

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

# FIXME: 1 min to wait + head checkout
# echo "Check python formatting with black" | ts
# ./check-black -n              |& tee /test_output/black_output.txt

echo "Check pylint" | ts
./check-pylint -n               |& tee /test_output/pylint_output.txt
echo "Check pylint. Done" | ts

echo "Check python type hinting with mypy" | ts
./check-mypy -n               |& tee /test_output/mypy_output.txt
echo "Check python type hinting with mypy. Done" | ts
