#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Each finished Iceberg data file used to keep its Parquet encoder threads until the commit, so writing
# 30 partitions with `max_threads = 16` needed 480 threads of the 300-thread global pool.
# https://github.com/ClickHouse/ClickHouse/issues/126508

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}"
trap 'rm -rf "${TABLE_DIR}"' EXIT

COLS=$(for i in $(seq 16); do printf ', c%d Int64' "$i"; done)
EXPRS=$(for i in $(seq 16); do printf ', number * %d' "$i"; done)
SETTINGS="SET allow_insert_into_iceberg = 1, max_insert_threads = 1, max_threads = 16, output_format_parquet_parallel_encoding = 1"

# INSERT, then ALTER TABLE ... UPDATE, which writes one position delete file and one data file per partition.
${CLICKHOUSE_LOCAL} --query "
    ${SETTINGS};
    CREATE TABLE t (p Int32 ${COLS}) ENGINE = IcebergLocal('${TABLE_DIR}/update/') PARTITION BY p;
    INSERT INTO t SELECT number % 30 ${EXPRS} FROM numbers(3000);
    SELECT count(), uniqExact(_path) FROM t;
    ALTER TABLE t UPDATE c1 = c1 + 1 WHERE 1;
    SELECT count(), sum(c1) FROM t;
" -- --max_thread_pool_size=300 --thread_pool_queue_size=300

# ALTER TABLE ... DELETE writes two-column position delete files, so it needs more partitions to fill the pool.
${CLICKHOUSE_LOCAL} --query "
    ${SETTINGS};
    CREATE TABLE t (p Int32, x Int64) ENGINE = IcebergLocal('${TABLE_DIR}/delete/') PARTITION BY p;
    INSERT INTO t SELECT number % 80, number FROM numbers(2000);
    ALTER TABLE t DELETE WHERE x >= 1000;
    SELECT count(), sum(x) FROM t;
" -- --max_thread_pool_size=300 --thread_pool_queue_size=300
