#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# column whose single value is wider than `max_compress_block_size`. The wide writer rounds the compress block
# size of a stream to whole values of its type (`roundCompressBlockSizeToWholeValues`), so a `FixedString` of
# 2 MiB under a 1 MiB setting gets a 2 MiB compressor block and a 2 MiB file buffer, and the readers of such a
# part see 2 MiB decompressed blocks. The estimate must price the rounded size, not the configured one.
#
# Two tables hold the same data volume in the same number of parts: one as 2 MiB values, the other as twice as
# many 1 MiB values, which fit the configured block exactly. Without the rounding both were priced the same.
#
# The reservation is observed the same way as in 05460_merge_memory_reservation_column_block_size_overrides: a
# background merge is held on the plain_merge_task_pause_before_prepare failpoint after it has reserved its
# estimate, and the MergesMutationsMemoryReservation metric is read while it waits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local value_size="$1"
    local rows_per_part="$2"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05878_merge_memory_reservation_fixed_string_XXXXXX")

    # Adaptive write buffers are disabled, so that every stream is priced at its full block size, and the merge
    # is kept horizontal, so that all streams are alive at once.
    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SET allow_suspicious_fixed_string_types = 1;
        CREATE TABLE t_merge_mem_fixed_string (k UInt64, v FixedString(${value_size}))
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 min_compress_block_size = 1048576, max_compress_block_size = 1048576,
                 min_columns_to_activate_adaptive_write_buffer = 0,
                 max_memory_ratio_to_activate_adaptive_write_buffer = 0,
                 vertical_merge_algorithm_min_rows_to_activate = 1000000000;

        INSERT INTO t_merge_mem_fixed_string SELECT number, toFixedString(toString(number), ${value_size}) FROM numbers(${rows_per_part});
        INSERT INTO t_merge_mem_fixed_string SELECT number, toFixedString(toString(number), ${value_size}) FROM numbers(${rows_per_part}, ${rows_per_part});
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_fixed_string MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

function reserved_for_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_merge "$1" "$2")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

# 8 values of 2 MiB per part: wider than the 1 MiB block.
wider=$(reserved_for_merge_with_retries 2097152 8)
# 16 values of 1 MiB per part: the same volume in values that fit the block.
fitting=$(reserved_for_merge_with_retries 1048576 16)

echo "$((fitting > 0))"
# The writer of the wider column allocates a 2 MiB compressor block and a 2 MiB file buffer instead of 1 MiB
# each, so its merge reserves at least 2 MiB more.
echo "$((wider >= fitting + 2097152))"
