#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
QUERIES=$BASEDIR/queries.txt

WORKDIR=$BASEDIR/`date +%Y-%m-%d_%H-%M-%S`
mkdir -p $WORKDIR
cd $WORKDIR

MAX_THREADS=8

echo "Working directory: $WORKDIR"
echo "Max threads: $MAX_THREADS"

echo -n "Start time: "
date '+%Y-%m-%d %H:%M:%S'

q_num=0
while IFS=';' read -r d q; do
    echo "Running Q${q_num} with delay $d seconds: $q"
    clickhouse-benchmark --precise --max_threads=$MAX_THREADS -d $d -c 1 -C 32 -q "$q" >Q${q_num}.log 2>&1
    q_num=$((q_num + 1))
done < "$QUERIES"

echo -n "End time: "
date '+%Y-%m-%d %H:%M:%S'
