#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 129827: a block in which a Nullable column holds only NULL takes no part in that column's bounds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# Writes the elements of array $3 into a new table $1 (x $2), one row per block, and prints the bounds of x in the
# manifest, each decoded by the expression $4 of the bound entry x.
check()
{
    echo "--- $2: $3"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE $1 (x $2) ENGINE = IcebergLocal('${ROOT}/$1')"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO $1 SELECT $3[number + 1] FROM numbers(length($3))
        SETTINGS allow_insert_into_iceberg = 1, max_block_size = 1, max_threads = 1, max_insert_threads = 1"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT arrayMap(x -> $4, tupleElement(data_file, 'lower_bounds')),
               arrayMap(x -> $4, tupleElement(data_file, 'upper_bounds'))
        FROM file('${ROOT}/$1/metadata/????????-*.avro', Avro)"
}

check t_middle 'Nullable(Int64)' '[1, NULL, 3]' 'reinterpretAsInt64(x.2)'
check t_first 'Nullable(Int64)' '[NULL, 2, 3]' 'reinterpretAsInt64(x.2)'
check t_string 'Nullable(String)' "['a', NULL, 'c']" 'x.2'
check t_all_null 'Nullable(Int64)' '[NULL, NULL]' 'reinterpretAsInt64(x.2)'
