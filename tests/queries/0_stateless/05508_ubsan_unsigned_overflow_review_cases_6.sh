#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Huge values of settings must not lead to unsigned integer overflows (checked by UBSan).
# The sanity checks of the settings write warnings to the logs: they are not interesting here.
CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=error/g')

# The number of streams of `zeros_mt` is limited by a `LIMIT` without multiplying it.
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM zeros_mt(2)
SETTINGS max_threads = 9223372036854775808, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 2, parallelize_output_from_storages = 0"

# The number of threads per remote shard is rounded up without overflowing.
$CLICKHOUSE_CLIENT -q "
SELECT dummy FROM remote('127.0.0.1', system.one)
SETTINGS max_threads = 18446744073709551615, prefer_localhost_replica = 0, enable_parallel_replicas = 0"

# The reservation of a spill of the radix `uniqExact` is checked.
$CLICKHOUSE_CLIENT -q "
SELECT uniqExact(number) FROM numbers(3)
SETTINGS count_distinct_optimization = 0, optimize_uniq_exact_radix_partitioning = 1, max_threads = 1,
    collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 1,
    max_bytes_ratio_before_external_group_by = 0, min_bytes_before_external_group_by = 0,
    min_free_disk_space_for_temporary_data = 18446744073709551615" 2>&1 | grep -q -E '^3$|NOT_ENOUGH_SPACE' && echo 'OK'

# The row group thresholds of `Parquet` output are compared without overflowing.
$CLICKHOUSE_CLIENT -q "
SELECT 1 AS x
SETTINGS output_format_parquet_row_group_size = 18446744073709551615, output_format_parquet_row_group_size_bytes = 1,
    output_format_parquet_parallel_encoding = 0
FORMAT Parquet" | $CLICKHOUSE_LOCAL --input-format Parquet -q "SELECT * FROM table"

# The stripes of the columns cache are compared with twice `max_block_size` without overflowing.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_stripe_overflow;
CREATE TABLE t_stripe_overflow (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_stripe_overflow SELECT number FROM numbers(256);
SELECT sum(x) FROM t_stripe_overflow
SETTINGS max_threads = 1, max_block_size = 9223372036854775808, preferred_block_size_bytes = 1,
    preferred_max_column_in_block_size_bytes = 0, use_columns_cache = 1, use_reader_executor = 0, enable_parallel_replicas = 0;
DROP TABLE t_stripe_overflow;"

# A huge lookahead of the userspace page cache saturates instead of wrapping around.
CONFIG_FILE="${CLICKHOUSE_TMP}/page_cache_config_${CLICKHOUSE_DATABASE}.yaml"
> "${CONFIG_FILE}" echo "
page_cache_max_size: 134217728
"
$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" -q "
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(100);
SELECT sum(x) FROM t
SETTINGS use_reader_executor = 0, use_page_cache_for_local_disks = 1, local_filesystem_read_method = 'pread',
    page_cache_lookahead_blocks = 18446744073709551615, use_uncompressed_cache = 0, use_columns_cache = 0;"
rm "${CONFIG_FILE}"
