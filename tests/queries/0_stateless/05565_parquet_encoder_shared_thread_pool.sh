#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires the Parquet format (USE_PARQUET build option)

# Every Parquet writer had its own pool of `max_threads` encoder threads, so many open writers with `max_threads = 32`
# did not fit in a 500-thread global pool: the 24 partitions of one stream.
# https://github.com/ClickHouse/ClickHouse/issues/129864
# `BackgroundSchedulePool` starts all of its threads at once in this release, so its default size of 512 does not fit the pool.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}"
trap 'rm -rf "${TABLE_DIR}"' EXIT

COLS=$(for i in $(seq 48); do printf ', c%d Int64' "$i"; done)
EXPRS=$(for i in $(seq 48); do printf ', number * %d' "$i"; done)
SETTINGS="SET max_threads = 32,
    output_format_parquet_parallel_encoding = 1, output_format_parquet_row_group_size = 200,
    max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0"

# Small row groups make every writer encode while the INSERT runs, not only when its file is finalized.
${CLICKHOUSE_LOCAL} --query "
    ${SETTINGS}, max_insert_threads = 1;
    INSERT INTO FUNCTION file('${TABLE_DIR}/file/{_partition_id}.parquet', 'Parquet', 'p UInt8 ${COLS}') PARTITION BY p
        SELECT number % 24 ${EXPRS} FROM numbers(4800);
    SELECT count(), sum(c48), uniqExact(_path) FROM file('${TABLE_DIR}/file/*.parquet');
" -- --max_thread_pool_size=500 --thread_pool_queue_size=500 --background_schedule_pool_size=16
