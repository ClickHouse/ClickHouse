#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on S3

# A Parquet reader of a file in object storage starts reading the column data of upcoming row groups
# before their decoding is scheduled, up to its share of max_download_threads. 02588_data.parquet has 35
# row groups; with one stream and max_parsing_threads = 4 the reader decodes one row group's column data
# at a time, so max_download_threads = 4 leaves room to read ahead and max_download_threads = 1 does not.
# Local files are not read ahead.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for i in 1 2; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}.parquet', format = 'RawBLOB') SETTINGS s3_truncate_on_insert = 1 FORMAT RawBLOB" < "$CURDIR/data_parquet/02588_data.parquet"
    $CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}.parquet', 'RawBLOB') SETTINGS engine_file_truncate_on_insert = 1 FORMAT RawBLOB" < "$CURDIR/data_parquet/02588_data.parquet"
done

query="SELECT count(), sum(seqnum), sum(length(market))"
s3_files="s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_{1,2}.parquet', format = 'Parquet')"
local_files="file('${CLICKHOUSE_TEST_UNIQUE_NAME}_{1,2}.parquet', 'Parquet')"
settings="max_threads = 1, max_parsing_threads = 4, input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = 4294967296, input_format_parquet_enable_row_group_prefetch = 1, enable_parallel_replicas = 0, use_page_cache_for_object_storage = 0"

$CLICKHOUSE_CLIENT --query "$query FROM $s3_files SETTINGS $settings, max_download_threads = 4, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_a_s3_4'"
$CLICKHOUSE_CLIENT --query "$query FROM $s3_files SETTINGS $settings, max_download_threads = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_b_s3_1'"
$CLICKHOUSE_CLIENT --query "$query FROM $local_files SETTINGS $settings, max_download_threads = 4, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_c_local_4'"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT substring(log_comment, length('${CLICKHOUSE_TEST_UNIQUE_NAME}_') + 1) AS read, ProfileEvents['ParquetColumnDataReadAheadRowGroups'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_')
    ORDER BY read"
