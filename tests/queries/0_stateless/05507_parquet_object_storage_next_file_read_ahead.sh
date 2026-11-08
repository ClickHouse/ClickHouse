#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# A source of an object storage reads its Parquet files one after another. From its third file on, while it reads a file,
# the next file's reader reads its offset index, without the column data next to it, and starts reading the column data of
# its first row group, without decoding it, if its whole column chunks fit (with the gaps between them when a chunk is
# shorter than remote_read_min_bytes_for_seek) within the size of one read task (4 x remote_read_min_bytes_for_seek) and
# in what the stream's ColumnData memory share leaves free. With such a short chunk, whose read takes the ranges near it,
# the row group is not read ahead if a later row group or PREWHERE step has ranges near it. Each file in good/ is one row
# group of ~247 KB. The rows are read slowly, so that the next file's reader has done its reads before the source switches to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ROWS=25000
WRITE="s3_truncate_on_insert = 1, output_format_parquet_row_group_size = $ROWS, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 1"

for i in 0 1 2 3 4 5; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/good/${i}.parquet', format = Parquet) SELECT number + ${i} * $ROWS AS x FROM numbers($ROWS) SETTINGS $WRITE"
done
for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/bad/${i}.parquet', format = Parquet) SELECT number + ${i} * $ROWS AS x FROM numbers($ROWS) SETTINGS $WRITE"
done
# The reader of this file fails in its initialization, when the file's schema is converted, because it has no column `x`.
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/bad/3.parquet', format = Parquet) SELECT number AS y FROM numbers($ROWS) SETTINGS $WRITE"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/not_parquet/0.parquet', format = Parquet) SELECT 1 AS x SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/not_parquet/1.parquet', format = RawBLOB) SELECT 'not a parquet file' SETTINGS s3_truncate_on_insert = 1"
for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/small/${i}.parquet', format = Parquet) SELECT number % 10 AS x FROM numbers($ROWS) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = $ROWS"
done
PLAIN="$WRITE, output_format_parquet_max_dictionary_size = 0, output_format_parquet_write_bloom_filter = 0"
# share/: two small files, then two without page index, whose chunks of 240 KB and 80 KB are each read in one request.
SHARE_ROWS=(1000 1000 30000 10000)
for i in 0 1 2 3; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/share/${i}.parquet', format = Parquet) SELECT number + ${i} * 30000 AS x FROM numbers(${SHARE_ROWS[$i]})
        SETTINGS $PLAIN, output_format_parquet_row_group_size = 30000, output_format_parquet_write_page_index = 0"
done
# In wide/, the unread column pad keeps the column data too far from the offset index to be read with it.
for i in 0 1 2 3; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/plain/${i}.parquet', format = Parquet) SELECT number + ${i} * $ROWS AS x FROM numbers($ROWS) SETTINGS $PLAIN"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/wide/${i}.parquet', format = Parquet) SELECT number + ${i} * 8000 AS c0, c0 AS c1, c0 AS c2, c0 AS c3, c0 AS c4, c0 AS c5, repeat('p', 10) AS pad FROM numbers(8000) SETTINGS $PLAIN"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/spans/${i}.parquet', format = Parquet) SELECT number + ${i} * 7000 AS c0, toUInt32(number) AS p0, c0 AS c1, toUInt32(number) AS p1, c0 AS c2, toUInt32(number) AS p2, c0 AS c3 FROM numbers(7000) SETTINGS $PLAIN"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/handback/${i}.parquet', format = Parquet) SELECT number + ${i} * 15000 AS x, toUInt16(number) AS pad, x AS z FROM numbers(15000) SETTINGS $PLAIN"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/rowgroups/${i}.parquet', format = Parquet) SELECT number + ${i} * 24000 AS x FROM numbers(24000)
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 3000, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 1,
            output_format_parquet_max_dictionary_size = 0, output_format_parquet_write_bloom_filter = 0"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/prewhere/${i}.parquet', format = Parquet) SELECT number + ${i} * 6000 AS x, number AS y FROM numbers(6000) SETTINGS $PLAIN"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/far/${i}.parquet', format = Parquet) SELECT number + ${i} * 15000 AS x, repeat('p', 50) AS pad FROM numbers(15000)
        SETTINGS $PLAIN, output_format_parquet_row_group_size = 5000, output_format_parquet_write_page_index = 0"
done

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ICEBERG_PATH}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_local (x Int64) ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet') SETTINGS allow_insert_into_iceberg = 1"
# One insert thread: parallel Iceberg sinks race on the next metadata file and log FILE_ALREADY_EXISTS.
for i in 0 1 2 3 4 5; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_local SELECT number + ${i} * $ROWS FROM numbers($ROWS) SETTINGS allow_insert_into_iceberg = 1, max_insert_threads = 1, output_format_parquet_compression_method = 'none'"
done

# The settings that decide whether the reads go to object storage in the background are pinned: one stream, a decoder
# thread pool, the threadpool read method, no page cache and no columns cache (they serve data without reading it).
SETTINGS="max_threads = 1, max_parsing_threads = 4, max_download_threads = 4, input_format_parquet_enable_row_group_prefetch = 1,
    remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, max_read_buffer_size = 1048576,
    max_read_buffer_size_remote_fs = 1048576, remote_read_min_bytes_for_seek = 65536,
    input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = 4294967296,
    use_page_cache_for_object_storage = 0, use_columns_cache = 0, use_query_condition_cache = 0, enable_parallel_replicas = 0"
QUERY="SELECT count(), sum(x), sum(sleepEachRow(0.000008))"

read_files()
{
    ${CLICKHOUSE_CLIENT} --query "$QUERY FROM $2 SETTINGS $SETTINGS, $3 log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1'"
}

read_files a_six_files "s3(s3_conn, filename = '${DIR}/good/*.parquet')" ""
read_files b_no_remote_prefetch "s3(s3_conn, filename = '${DIR}/good/*.parquet')" "remote_filesystem_read_prefetch = 0,"
# A read task of 64 KB does not hold the 247 KB column chunk.
read_files c_larger_than_read_task "s3(s3_conn, filename = '${DIR}/good/{0,1,2,3}.parquet')" "remote_read_min_bytes_for_seek = 16384,"
# The ColumnData share of the stream is 500000 / 5 = 100 KB.
read_files d_larger_than_memory_share "s3(s3_conn, filename = '${DIR}/good/{0,1,2,3}.parquet')" "input_format_parquet_memory_high_watermark = 500000,"
read_files e_two_files "s3(s3_conn, filename = '${DIR}/good/{0,1}.parquet')" ""
read_files f_local_object_storage "t_local" ""
# The third file is read ahead before the source switches to it, so its 240 KB chunk counts against the share of the
# fourth file: g 300 KB, which refuses it, h default. The third file's rows are read slowly, so it holds that chunk.
SHARE="SELECT count(), sum(x), sum(sleepEachRow(0.00004)) FROM s3(s3_conn, filename = '${DIR}/share/*.parquet')
    SETTINGS $SETTINGS, input_format_parquet_max_block_size = 4096, use_parquet_metadata_cache = 0"
${CLICKHOUSE_CLIENT} --query "$SHARE, input_format_parquet_memory_high_watermark = 1500000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_g_shared_share_refused'"
${CLICKHOUSE_CLIENT} --query "$SHARE, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_h_shared_share_control'"
# Not read ahead: without a decoder thread pool, for files smaller than remote_read_min_bytes_for_seek, with TopN filtering.
read_files i_manual_parsing "s3(s3_conn, filename = '${DIR}/good/{0,1,2}.parquet')" "max_parsing_threads = 1, input_format_parquet_max_block_size = 8192,"
read_files j_small_files "s3(s3_conn, filename = '${DIR}/small/{0,1,2}.parquet')" ""
${CLICKHOUSE_CLIENT} --query "SELECT x, sleepEachRow(0.000008) FROM s3(s3_conn, filename = '${DIR}/good/{0,1,2}.parquet') ORDER BY x DESC LIMIT 1
    SETTINGS $SETTINGS, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_execute_functions_after_sorting = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_k_top_n'"
# Read ahead through the wrapper of aggregate_function_input_format = 'value'.
${CLICKHOUSE_CLIENT} --query "SELECT count(), sumMerge(x), sum(sleepEachRow(0.000008)) FROM s3(s3_conn, filename = '${DIR}/good/*.parquet', structure = 'x AggregateFunction(sum, UInt64)')
    SETTINGS $SETTINGS, aggregate_function_input_format = 'value', log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_l_aggregate_states'"
# A refused file reads its offset index ahead without the column data next to it, which is read later in a request of its own.
read_files m_plain_refused "s3(s3_conn, filename = '${DIR}/plain/*.parquet', structure = 'x UInt64')" "input_format_parquet_memory_high_watermark = 500000, use_parquet_metadata_cache = 0,"
read_files n_plain_refused_control "s3(s3_conn, filename = '${DIR}/plain/*.parquet', structure = 'x UInt64')" "input_format_parquet_memory_high_watermark = 500000, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 0,"
# A first row group larger than one read task is not read ahead, also when each of its chunks is shorter than remote_read_min_bytes_for_seek.
WIDE="SELECT c0 + c1 + c2 + c3 + c4 + c5, sleepEachRow(0.00004) FROM s3(s3_conn, filename = '${DIR}/wide/*.parquet', structure = 'c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64') LIMIT 20000"
${CLICKHOUSE_CLIENT} --query "$WIDE SETTINGS $SETTINGS, use_parquet_metadata_cache = 0, input_format_parquet_max_block_size = 1000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_o_wide_refused' FORMAT Null"
${CLICKHOUSE_CLIENT} --query "$WIDE SETTINGS $SETTINGS, use_parquet_metadata_cache = 0, input_format_parquet_max_block_size = 1000, remote_filesystem_read_prefetch = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_p_wide_refused_control' FORMAT Null"
# A first row group whose short chunks fit in one read task, but not with the gaps their reads span, is not read ahead.
SPANS="SELECT c0 + c1 + c2 + c3, sleepEachRow(0.00004) FROM s3(s3_conn, filename = '${DIR}/spans/*.parquet', structure = 'c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64') LIMIT 18000"
${CLICKHOUSE_CLIENT} --query "$SPANS SETTINGS $SETTINGS, use_parquet_metadata_cache = 0, input_format_parquet_max_block_size = 1000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_q_spans_refused' FORMAT Null"
${CLICKHOUSE_CLIENT} --query "$SPANS SETTINGS $SETTINGS, use_parquet_metadata_cache = 0, input_format_parquet_max_block_size = 1000, remote_filesystem_read_prefetch = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_r_spans_refused_control' FORMAT Null"
# The pages of a first row group whose chunks fit in one read task, but whose read task over an unread column does not, are handed back.
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x), sum(z), sum(sleepEachRow(0.000008)) FROM s3(s3_conn, filename = '${DIR}/handback/*.parquet', structure = 'x UInt64, pad UInt16, z UInt64')
    SETTINGS $SETTINGS, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_s_handed_back'"
# A short chunk's read takes the ranges near it, so a first row group with one is not read ahead when a later row group or PREWHERE step has ranges near it.
ROW_GROUPS="SELECT x, sleepEachRow(0.00001) FROM s3(s3_conn, filename = '${DIR}/rowgroups/*.parquet', structure = 'x UInt64') LIMIT 64000"
${CLICKHOUSE_CLIENT} --query "$ROW_GROUPS SETTINGS $SETTINGS, use_parquet_metadata_cache = 0, input_format_parquet_max_block_size = 1000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_t_row_groups_refused' FORMAT Null"
${CLICKHOUSE_CLIENT} --query "$ROW_GROUPS SETTINGS $SETTINGS, use_parquet_metadata_cache = 0, input_format_parquet_max_block_size = 1000, remote_filesystem_read_prefetch = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_u_row_groups_refused_control' FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(y), sum(sleepEachRow(0.00004)) FROM s3(s3_conn, filename = '${DIR}/prewhere/*.parquet', structure = 'x UInt64, y UInt64') PREWHERE x % 2 = 0
    SETTINGS $SETTINGS, input_format_parquet_page_filter_push_down = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_v_prewhere_refused'"
# Nor with the columns cache, whose entries the initialization of the next file would hold.
read_files w_columns_cache "s3(s3_conn, filename = '${DIR}/good/*.parquet')" "use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,"
# In far/, the unread column pad keeps the 40 KB chunks of the later row groups far from that of the first, which is read ahead.
read_files x_far_row_groups "s3(s3_conn, filename = '${DIR}/far/*.parquet', structure = 'x UInt64')" "use_parquet_metadata_cache = 0,"

# The error of the fourth file is reported with its name, as when it is not read ahead.
for prefetch in 1 0; do
    error=$(${CLICKHOUSE_CLIENT} --query "$QUERY FROM s3(s3_conn, filename = '${DIR}/bad/*.parquet', structure = 'x UInt64')
        SETTINGS $SETTINGS, input_format_parquet_allow_missing_columns = 0, remote_filesystem_read_prefetch = $prefetch" 2>&1)
    echo "$(grep -o -m1 THERE_IS_NO_COLUMN <<< "$error") $(grep -q -F "${DIR}/bad/3.parquet" <<< "$error" && echo 1 || echo 0)"
done
# So is the error of a footer that is read ahead.
error=$(${CLICKHOUSE_CLIENT} --query "SELECT sum(x) FROM s3(s3_conn, filename = '${DIR}/not_parquet/*.parquet') SETTINGS max_threads = 1" 2>&1)
echo "$(grep -o -m1 INCORRECT_DATA <<< "$error") $(grep -q -F "${DIR}/not_parquet/1.parquet" <<< "$error" && echo 1 || echo 0)"

# Nor through the page cache. The CI server has none, so a clickhouse-local reads with one, then without it as a control.
config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache.yaml"
echo "page_cache_max_size: 134217728" > "$config"
url_files="s3('http://localhost:11111/test/${DIR}/good/{0,1,2}.parquet', 'test', 'testtest', 'Parquet')"
events="SELECT sumIf(value, event = 'PageCacheReadBytes') > 0, sumIf(value, event = 'ParquetColumnDataReadAheadOfNextFile') > 0 FROM system.events"
${CLICKHOUSE_LOCAL} --config-file "$config" --multiquery "
    $QUERY FROM $url_files SETTINGS $SETTINGS, use_page_cache_for_object_storage = 1 FORMAT Null;
    $events;
    $QUERY FROM $url_files SETTINGS $SETTINGS FORMAT Null;
    $events;"
rm -f "$config"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
LOG="FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_')"
# The number of files read ahead for g and h, whether any is for the others.
${CLICKHOUSE_CLIENT} --query "
    SELECT substring(log_comment, length('${CLICKHOUSE_TEST_UNIQUE_NAME}_') + 1) AS read,
        if(read IN ('g_shared_share_refused', 'h_shared_share_control'), ProfileEvents['ParquetColumnDataReadAheadOfNextFile'] AS files, files > 0)
    $LOG
    ORDER BY read
    SETTINGS enable_parallel_replicas = 0"
${CLICKHOUSE_CLIENT} --query "
    SELECT sumIf(ProfileEvents['ParquetPrefetcherReadRandomRead'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_m_plain_refused')
        - sumIf(ProfileEvents['ParquetPrefetcherReadRandomRead'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_n_plain_refused_control')
    $LOG
    SETTINGS enable_parallel_replicas = 0"
${CLICKHOUSE_CLIENT} --query "
    SELECT sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_o_wide_refused')
        - sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_p_wide_refused_control') < 100000
    $LOG
    SETTINGS enable_parallel_replicas = 0"
${CLICKHOUSE_CLIENT} --query "
    SELECT sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_q_spans_refused')
        - sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_r_spans_refused_control') < 100000
    $LOG
    SETTINGS enable_parallel_replicas = 0"
${CLICKHOUSE_CLIENT} --query "
    SELECT sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_t_row_groups_refused')
        - sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_u_row_groups_refused_control') < 100000
    $LOG
    SETTINGS enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_local"
rm -rf "${ICEBERG_PATH}"
