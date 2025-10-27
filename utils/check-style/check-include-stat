#!/bin/sh
set -e

CUR_DIR=`dirname $0`
CUR_DIR=`readlink -f $CUR_DIR`
CUR_DIR="${CUR_DIR}/"

RESULT_FILE=${RESULT_FILE:=${CUR_DIR}check-include.log}
finish() {
    echo include check failed:
    cat $RESULT_FILE
}
trap finish 0 1 3 6 15

sh ${CUR_DIR}check-include > $RESULT_FILE 2>&1

echo Results:
echo Top by memory:
cat $RESULT_FILE | sort -nrk4 | head -n20

echo Top by time:
cat $RESULT_FILE | sort -nrk3 | head -n20

echo Top by includes:
cat $RESULT_FILE | sort -nrk2 | head -n20

trap "" EXIT
