#!/usr/bin/env bash
# Tags: no-fasttest
# INSERT starts a new data file when a chunk's first sort key sorts before the previous chunk's last key.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Prints the number of data files and how many of them are not sorted.
function check()
{
    local name="$1" columns="$2" order_by="$3" key="$4" sort_function="$5" select="$6"
    local table="t_${CLICKHOUSE_DATABASE}_${name}_${RANDOM}"
    local table_path="${USER_FILES_PATH}/${table}/"

    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (${columns}) ENGINE = IcebergLocal('${table_path}', 'Parquet') ORDER BY ${order_by}"
    # One chunk per 1000 rows, all on one sink.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_threads=1 --max_insert_threads=1 \
        --max_block_size=1000 --max_insert_block_size=1000 --min_insert_block_size_rows=0 --min_insert_block_size_bytes=0 \
        --query "INSERT INTO ${table} ${select}"
    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "
        SELECT '${name}', count(), countIf(keys != ${sort_function}(keys))
        FROM (SELECT groupArray(${key}) AS keys FROM ${table} GROUP BY _file)"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
    rm -rf "${table_path}"
}

check descending_input 'x Int64' 'x' 'x' arraySort 'SELECT 9999 - number FROM numbers(10000)'
check ascending_input 'x Int64' 'x' 'x' arraySort 'SELECT number FROM numbers(10000)'
check two_columns 'a Int64, b Int64' '(a, b)' '(a, b)' arraySort 'SELECT intDiv(number, 1000), 999 - (number % 1000) FROM numbers(10000)'
check desc_order_ascending_input 'x Int64' 'x DESC' 'x' arrayReverseSort 'SELECT number FROM numbers(10000)'
