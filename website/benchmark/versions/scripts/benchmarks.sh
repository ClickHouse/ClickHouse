#!/usr/bin/env bash

CH_QUERIES_FILE="ch_queries.sql"
SSB_QUERIES_FILE="ssb_queries.sql"
BROWN_QUERIES_FILE="brown_queries.sql"
CH_TABLE="hits_100m_obfuscated"
TRIES=3

if [ -x ./clickhouse ]
then
    CLICKHOUSE_CLIENT="./clickhouse client"
elif command -v clickhouse-client >/dev/null 2>&1
then
    CLICKHOUSE_CLIENT="clickhouse-client"
else
    echo "clickhouse-client is not found"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query 'SELECT version();'

echo "Brown Benchmark:"

cat "$BROWN_QUERIES_FILE" | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(${CLICKHOUSE_CLIENT} --time --format=Null --max_memory_usage=100G --query="$query" 2>&1)
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done

echo "SSB Benchmark:"

cat "$SSB_QUERIES_FILE" | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(${CLICKHOUSE_CLIENT} --time --format=Null --max_memory_usage=100G --query="$query" 2>&1)
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done


echo "ClickHouse Benchmark:"

cat "$CH_QUERIES_FILE" | sed "s/{table}/${CH_TABLE}/g" | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(${CLICKHOUSE_CLIENT} --time --format=Null --max_memory_usage=100G --query="$query" 2>&1)
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done
