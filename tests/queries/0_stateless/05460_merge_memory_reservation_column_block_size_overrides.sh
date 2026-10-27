#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) with
# column-level `max_compress_block_size` overrides. The wide writer sizes the eager buffers of a stream by the
# column's own override and falls back to the table setting only for a column without one. When EVERY written
# column overrides the table setting with a smaller value, the table setting is never used, so the merge must
# reserve about the same as a table whose setting is that smaller value - not price every stream at the
# (much larger) table setting.
#
# The reservation is observed the same way as in 05218_merge_memory_reservation_nested_shared_offsets: a
# background merge is held on the plain_merge_task_pause_before_prepare failpoint after it has reserved its
# estimate, and the MergesMutationsMemoryReservation metric is read while it waits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local table_block_size="$1"
    local column_settings="$2"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05460_merge_memory_reservation_block_size_XXXXXX")

    local columns="k UInt64 ${column_settings}"
    local values="number"
    for i in $(seq 1 20)
    do
        columns="${columns}, c${i} UInt64 ${column_settings}"
        values="${values}, number + ${i}"
    done

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_block_size (${columns})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, max_compress_block_size = ${table_block_size},
            min_columns_to_activate_adaptive_write_buffer = 0, max_memory_ratio_to_activate_adaptive_write_buffer = 0;

        INSERT INTO t_merge_mem_block_size SELECT ${values} FROM numbers(2);
        INSERT INTO t_merge_mem_block_size SELECT ${values} FROM numbers(2, 2);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_block_size MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

# Every column overrides the 64 MiB table setting with 1 MiB.
overridden=$(reserved_for_merge_with_retries 67108864 "SETTINGS (max_compress_block_size = 1048576)")
# The same columns with a 1 MiB table setting and no overrides.
plain=$(reserved_for_merge_with_retries 1048576 "")

echo "$((plain > 0))"
# Before the fix the overridden table reserved 64 MiB eager buffers per stream (more than 40 times as much).
echo "$((overridden < 2 * plain))"
