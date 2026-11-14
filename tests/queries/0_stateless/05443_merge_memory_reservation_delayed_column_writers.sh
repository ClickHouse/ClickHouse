#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (minio)

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# delayed column writers of a vertical merge. On a disk with parallel write, MergeTask keeps up to
# max_merge_delayed_streams_for_parallel_write finished gathering column WRITERS alive, and each of them keeps
# all the streams of its column. A Tuple of 16 elements has 16 streams, so 41 gathering Tuple columns keep
# 40 * 16 delayed streams alive, while 41 gathering UInt8 columns keep only 40. Capping the number of delayed
# STREAMS at the limit priced both merges almost the same.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local column_type="$1"
    local tag="$2"
    local column_definitions data_dir
    column_definitions=$(seq -s ', ' -f "c%g ${column_type}" 1 41)
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05443_merge_memory_reservation_delayed_column_writers_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_delayed (k UInt64, ${column_definitions})
        ENGINE = MergeTree ORDER BY k
        SETTINGS disk = disk(
                     type = s3,
                     endpoint = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_${tag}/',
                     access_key_id = 'clickhouse',
                     secret_access_key = 'clickhouse'),
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_compress_block_size = 1048576,
                 min_columns_to_activate_adaptive_write_buffer = 0, use_adaptive_write_buffer_for_dynamic_subcolumns = 0,
                 max_merge_delayed_streams_for_parallel_write = 40,
                 enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_bytes_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_delayed (k) SELECT number FROM numbers(100);
        INSERT INTO t_merge_mem_delayed (k) SELECT number FROM numbers(100, 100);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_delayed MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

tuple_columns=$(reserved_for_merge_with_retries "Tuple($(seq -s ', ' -f 'e%g UInt8' 1 16))" tuple)
scalar_columns=$(reserved_for_merge_with_retries UInt8 scalar)

echo "$((scalar_columns > 0))"
# 40 delayed writers of 15 extra streams each, every stream with at least a 1 MiB compressor block and a
# 1 MiB file buffer: at least 1200 MiB more. Capping the delayed streams priced only the widest gathering
# column's 15 extra streams.
echo "$((tuple_columns - scalar_columns > 1000 * 1048576))"
