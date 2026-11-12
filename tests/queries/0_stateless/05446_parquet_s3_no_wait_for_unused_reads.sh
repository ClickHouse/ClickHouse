#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas, no-distributed-cache
# no-fasttest: needs S3.
# no-parallel-replicas: `s3` would become `s3Cluster`, and the file would be read on another node.
# no-distributed-cache: the timings depend on the read path.

# Row group 0 of the file is tiny, row groups 1-4 are 16 MiB each, and reads are throttled to 8 MiB/s.
# The background reads of row groups 1-4 must not delay the end of a query whose `LIMIT` row group 0
# satisfies, nor the end of a query cancelled by `max_execution_time`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3="s3('http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet', 'test', 'testtest', 'Parquet', 'a UInt64')"
R=2097152
SETTINGS="max_threads = 4, max_download_threads = 4, max_block_size = 65536, max_remote_read_network_bandwidth = 8388608,
    remote_filesystem_read_method = 'threadpool', use_page_cache_for_object_storage = 0, enable_filesystem_cache = 0,
    input_format_parquet_enable_row_group_prefetch = 1, use_parquet_metadata_cache = 0, use_columns_cache = 0,
    optimize_count_from_files = 1, use_cache_for_count_from_files = 1"
Q="${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION $S3 SELECT if(number < $R, 0, sipHash64(number)) AS a FROM numbers(5 * $R)
    SETTINGS s3_truncate_on_insert = 1, max_block_size = 65536, output_format_parquet_row_group_size = $R,
    output_format_parquet_write_page_index = 1, output_format_parquet_write_bloom_filter = 0"

# The same rows, with one parsing thread, which reads nothing but row group 0.
${CLICKHOUSE_CLIENT} --query_id "${Q}_m" -q "SELECT * FROM $S3 LIMIT 2 SETTINGS max_parsing_threads = 24, $SETTINGS FORMAT Null"
${CLICKHOUSE_CLIENT} --query_id "${Q}_c" -q "SELECT * FROM $S3 LIMIT 2 SETTINGS max_parsing_threads = 1, $SETTINGS FORMAT Null"

# The connection takes the next query only after the timed out one is torn down.
${CLICKHOUSE_CLIENT} --ignore-error -q "
    SELECT * FROM $S3 SETTINGS max_parsing_threads = 24, $SETTINGS, max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '${Q}_t' FORMAT Null;
    SELECT 1 SETTINGS log_comment = '${Q}_t_next' FORMAT Null" 2>/dev/null

# With one parsing thread the query thread itself waits for the read of row group 1, which takes 8 s at 2 MiB/s.
${CLICKHOUSE_CLIENT} --ignore-error -q "
    SELECT * FROM $S3 SETTINGS max_parsing_threads = 1, $SETTINGS, max_remote_read_network_bandwidth = 2097152,
        max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '${Q}_t1' FORMAT Null;
    SELECT 1 SETTINGS log_comment = '${Q}_t1_next' FORMAT Null" 2>/dev/null

${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        count(),
        min(result_rows),
        maxIf(ProfileEvents['S3GetObject'], query_id = '${Q}_m') > maxIf(ProfileEvents['S3GetObject'], query_id = '${Q}_c'),
        maxIf(query_duration_ms, query_id = '${Q}_m') <= maxIf(query_duration_ms, query_id = '${Q}_c') + 3000
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id IN ('${Q}_m', '${Q}_c') AND type = 'QueryFinish';
    SELECT
        count(),
        anyIf(exception_code, log_comment = '${Q}_t'),
        dateDiff('ms', anyIf(query_start_time_microseconds, log_comment = '${Q}_t'),
            anyIf(query_start_time_microseconds, log_comment = '${Q}_t_next')) <= 2000 + 2500
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment IN ('${Q}_t', '${Q}_t_next') AND type IN ('ExceptionWhileProcessing', 'QueryFinish');
    SELECT
        count(),
        anyIf(exception_code, log_comment = '${Q}_t1'),
        dateDiff('ms', anyIf(query_start_time_microseconds, log_comment = '${Q}_t1'),
            anyIf(query_start_time_microseconds, log_comment = '${Q}_t1_next')) <= 2000 + 2500
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment IN ('${Q}_t1', '${Q}_t1_next') AND type IN ('ExceptionWhileProcessing', 'QueryFinish')"

# The cancelled read has not reached the end of the file, so it must not have cached a row count.
${CLICKHOUSE_CLIENT} -q "SELECT count() = 5 * $R FROM $S3 SETTINGS $SETTINGS"
