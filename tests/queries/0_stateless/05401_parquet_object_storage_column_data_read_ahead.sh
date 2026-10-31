#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on S3

# A Parquet reader of a file in object storage starts reading the column data of upcoming row groups
# before their decoding is scheduled, up to its share of max_download_threads. 02588_data.parquet has 35
# row groups; with one stream and max_parsing_threads = 4 the reader decodes one row group's column data
# at a time, so max_download_threads = 4 leaves room to read ahead and max_download_threads = 1 does not.
# With as many streams as max_download_threads (k: two), each reader's share is the row group it decodes, yet
# it keeps one small row group ahead (_gaps.parquet: ~9 KB chunks of x between ~64 KB chunks of an unread
# column), but not with max_download_threads = 1 (m). One stream with max_parsing_threads = 16 and
# max_download_threads = 2 is the same case throughout (with two streams, the one left when the other
# finishes gets a larger share and reads ahead normally): it reads no row group ahead that admission would
# read together with the next ones (l, _adj.parquet: adjacent ~16 KB row groups), since reading it alone
# would split that request, nor a read above remote_read_min_bytes_for_seek (n: ~9 KB chunks of
# _gaps_1.parquet over a 4 KB threshold).
# Local files are not read ahead. The columns cache is off: the columns it serves have no column data to read.
# With max_parsing_threads = 16 four decoders admit row groups concurrently while reading ahead.
# Read-ahead takes at most the column data stage's share (0.2) of the memory the query may still use:
# the ~8 MB second row group of _big.parquet is not read ahead under a 32 MB limit, but is under 1 GB.
# Column chunks with an offset index (_pi.parquet) are not read ahead.
# Small row groups (_small.parquet, ~2.4 KB chunks) are read ahead in requests of up to 4 x
# remote_read_min_bytes_for_seek, also when one of their two columns is not read. Row groups of at least
# remote_read_min_bytes_for_seek / 16 (_medium.parquet, ~16 KB chunks) keep one request each; with
# max_download_threads = 64 read-ahead starts them before admission can coalesce them. Both files are
# larger than max_read_buffer_size, which would prefetch a whole file in one request.
# Reads through the userspace page cache are not read ahead. The CI server has no page cache, so a
# clickhouse-local with one reads the files, then reads them again without it as a control.
# A read buffer larger than the file puts the asynchronous prefetch buffer above the page cache
# (RemoteFSBuffers); those reads are not read ahead either. The structure is given because schema
# inference opens such a buffer too.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for i in 1 2; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}.parquet', format = 'RawBLOB') SETTINGS s3_truncate_on_insert = 1 FORMAT RawBLOB" < "$CURDIR/data_parquet/02588_data.parquet"
    $CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}.parquet', 'RawBLOB') SETTINGS engine_file_truncate_on_insert = 1 FORMAT RawBLOB" < "$CURDIR/data_parquet/02588_data.parquet"
done
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_big.parquet', format = 'Parquet') SELECT if(number < 1000000, 0, number) AS x FROM numbers(2000000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 0"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_pi.parquet', format = 'Parquet') SELECT number AS x FROM numbers(200000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000, output_format_parquet_write_page_index = 1"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_small.parquet', format = 'Parquet') SELECT number AS x, number AS y FROM numbers(90000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 300, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_medium.parquet', format = 'Parquet') SELECT number AS x FROM numbers(200000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 2000, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0"
for i in 1 2; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_gaps_${i}.parquet', format = 'Parquet') SELECT number AS x, leftPad(toString(number), 60, '0') AS pad FROM numbers(40000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0"
done
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_adj.parquet', format = 'Parquet') SELECT number AS x FROM numbers(200000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 2000, output_format_parquet_compression_method = 'none', output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0"

query="SELECT count(), sum(seqnum), sum(length(market))"
s3_files="s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_{1,2}.parquet', format = 'Parquet')"
local_files="file('${CLICKHOUSE_TEST_UNIQUE_NAME}_{1,2}.parquet', 'Parquet')"
settings="max_threads = 1, max_parsing_threads = 4, input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = 4294967296, input_format_parquet_enable_row_group_prefetch = 1, enable_parallel_replicas = 0, use_page_cache_for_object_storage = 0, use_columns_cache = 0"

$CLICKHOUSE_CLIENT --query "$query FROM $s3_files SETTINGS $settings, max_download_threads = 4, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_a_s3_4'"
$CLICKHOUSE_CLIENT --query "$query FROM $s3_files SETTINGS $settings, max_download_threads = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_b_s3_1'"
gaps_files="s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_gaps_{1,2}.parquet', format = 'Parquet', structure = 'x UInt64')"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM $gaps_files SETTINGS ${settings/max_threads = 1/max_threads = 2}, max_download_threads = 2, remote_read_min_bytes_for_seek = 32768, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_k_s3_2_streams_2'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM $gaps_files SETTINGS ${settings/max_threads = 1/max_threads = 2}, max_download_threads = 1, remote_read_min_bytes_for_seek = 32768, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_m_s3_2_streams_1'"
$CLICKHOUSE_CLIENT --query "$query FROM $local_files SETTINGS $settings, max_download_threads = 4, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_c_local_4'"
settings_16="max_threads = 1, max_parsing_threads = 16, input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = 4294967296, input_format_parquet_enable_row_group_prefetch = 1, enable_parallel_replicas = 0, use_page_cache_for_object_storage = 0, use_columns_cache = 0"
$CLICKHOUSE_CLIENT --query "$query FROM $s3_files SETTINGS $settings_16, max_download_threads = 16, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_g_s3_16_decoders'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_gaps_1.parquet', format = 'Parquet', structure = 'x UInt64') SETTINGS $settings_16, max_download_threads = 2, remote_read_min_bytes_for_seek = 4096, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_n_s3_1_stream_large_reads'"

big_file="s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_big.parquet', format = 'Parquet')"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM $big_file SETTINGS $settings, max_download_threads = 4, max_memory_usage = 32000000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_d_s3_limit_32mb'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM $big_file SETTINGS $settings, max_download_threads = 4, max_memory_usage = 1000000000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_e_s3_limit_1gb'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_pi.parquet', format = 'Parquet') SETTINGS $settings, max_download_threads = 4, max_memory_usage = 1000000000, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_f_s3_offset_index'"
small_file="s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_small.parquet', format = 'Parquet', structure = 'x UInt64, y UInt64')"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x), sum(y) FROM $small_file SETTINGS $settings, max_download_threads = 4, remote_read_min_bytes_for_seek = 131072, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_h_s3_small_row_groups'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM $small_file SETTINGS $settings, max_download_threads = 4, remote_read_min_bytes_for_seek = 131072, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_i_s3_small_row_groups_one_column'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_medium.parquet', format = 'Parquet', structure = 'x UInt64') SETTINGS $settings, max_download_threads = 64, remote_read_min_bytes_for_seek = 131072, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_j_s3_medium_row_groups'"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM s3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}_adj.parquet', format = 'Parquet', structure = 'x UInt64') SETTINGS $settings_16, max_download_threads = 2, remote_read_min_bytes_for_seek = 131072, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_l_s3_1_stream_adjacent'"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['ParquetColumnDataReadAheadRowGroups'] * 2 > ProfileEvents['ParquetReadRowGroups']
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_k_s3_2_streams_2'
    SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "
    SELECT substring(log_comment, length('${CLICKHOUSE_TEST_UNIQUE_NAME}_') + 1) AS read, ProfileEvents['ParquetColumnDataReadAheadRowGroups'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_')
    ORDER BY read
    SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "
    SELECT sumIf(ProfileEvents['S3GetObject'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_h_s3_small_row_groups') < 10,
        sumIf(ProfileEvents['S3GetObject'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_i_s3_small_row_groups_one_column') < 10,
        sumIf(ProfileEvents['S3GetObject'], log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_j_s3_medium_row_groups') > 40
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_h_s3_small_row_groups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_i_s3_small_row_groups_one_column',
            '${CLICKHOUSE_TEST_UNIQUE_NAME}_j_s3_medium_row_groups')
    SETTINGS enable_parallel_replicas = 0"

config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache.yaml"
echo "page_cache_max_size: 134217728" > "$config"
s3_url_files="s3('http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}_{1,2}.parquet', 'test', 'testtest', 'Parquet', 'seqnum Nullable(UInt32), market Nullable(String)')"
events="SELECT sumIf(value, event = 'PageCacheReadBytes') > 0, sumIf(value, event = 'RemoteFSBuffers') > 0, sumIf(value, event = 'ParquetColumnDataReadAheadRowGroups') > 0 FROM system.events"
page_cache_settings="${settings/use_page_cache_for_object_storage = 0/use_page_cache_for_object_storage = 1}, max_download_threads = 4"
async_settings="max_read_buffer_size_remote_fs = 16777216, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1"
$CLICKHOUSE_LOCAL --config-file "$config" --multiquery "
    $query FROM $s3_url_files SETTINGS $page_cache_settings FORMAT Null;
    $events;
    $query FROM $s3_url_files SETTINGS $page_cache_settings, $async_settings FORMAT Null;
    $events;
    $query FROM $s3_url_files SETTINGS $settings, max_download_threads = 4 FORMAT Null;
    $events;"
rm -f "$config"
