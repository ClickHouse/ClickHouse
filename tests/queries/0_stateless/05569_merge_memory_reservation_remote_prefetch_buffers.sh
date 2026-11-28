#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (minio)

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# readers of remote parts. With remote_filesystem_read_method = 'threadpool' and remote_filesystem_read_prefetch,
# a reader is an AsynchronousBoundedReadBuffer that swaps its filled prefetch buffer into the working memory and
# allocates another one for the next prefetch, so each reader stream holds two file buffers, not one.
#
# The parts are large and incompressible, so the reader term is not capped by the data volume (a reader stream is
# also charged its decompressed block and a retained copy of a compressed block, so every `String` column is kept
# in a single stream to leave enough data per stream). The same merge
# is priced with and without the remote prefetch: 2 parts x 10 columns hold 20 reader streams, and each of them
# must be charged one more 1 MiB file buffer when the prefetch is on.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local prefetch="$1"
    local tag="$2"
    local column_definitions column_values data_dir
    column_definitions=$(seq -s ', ' -f "c%g String" 1 10)
    column_values=$(seq -s ', ' -f "randomString(1000) AS v%g" 1 10)
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05569_merge_memory_reservation_remote_prefetch_buffers_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_prefetch (k UInt64, ${column_definitions})
        ENGINE = MergeTree ORDER BY k
        SETTINGS disk = disk(
                     type = s3,
                     endpoint = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_${tag}/',
                     access_key_id = 'clickhouse',
                     secret_access_key = 'clickhouse'),
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_compress_block_size = 1048576,
                 string_serialization_version = 'single_stream',
                 enable_vertical_merge_algorithm = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_prefetch SELECT number, ${column_values} FROM numbers(5000);
        INSERT INTO t_merge_mem_prefetch SELECT number, ${column_values} FROM numbers(5000, 5000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" \
        --max_read_buffer_size 1048576 --remote_filesystem_read_method threadpool --remote_filesystem_read_prefetch "$prefetch" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_prefetch MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

function reserved_for_merge_with_retries()
{
    local result=0
    for attempt in 1 2 3
    do
        result=$(reserved_for_merge "$1" "$2_$attempt")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

with_prefetch=$(reserved_for_merge_with_retries 1 prefetch)
without_prefetch=$(reserved_for_merge_with_retries 0 no_prefetch)

echo "$((without_prefetch > 0))"
# 20 reader streams, each with one more 1 MiB file buffer when the remote reader prefetches.
echo "$((with_prefetch - without_prefetch >= 20 * 1048576))"
