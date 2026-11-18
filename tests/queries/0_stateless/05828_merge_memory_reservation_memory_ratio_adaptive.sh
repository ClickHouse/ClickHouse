#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the failpoint used to park the merge is not available in the fast test build

# The merge memory reservation (`CompactionStatistics::estimateNeededMemoryForMerge`) must mirror the
# memory-ratio trigger of adaptive write buffers (`max_memory_ratio_to_activate_adaptive_write_buffer`, see
# `writeBuffersWouldTakeSignificantMemory`): when the eager buffers of a writer's streams would take that share of
# the server memory limit, every stream starts at `adaptive_write_buffer_initial_size`, and the reservation must
# price them at that size instead of at two full `max_compress_block_size` buffers per stream.
#
# The server memory limit is fixed for the clickhouse-local process, the count-based rule is disabled, and the
# same table and data are merged with the ratio trigger on and off. 21 streams with 4 MiB blocks preallocate
# about 168 MiB, which is above the 0.002 share (about 131 MiB) of the 64 GiB limit, so with the trigger the eager
# buffers almost vanish.
#
# The reservation is read from the `MergesMutationsMemoryReservation` metric while the selected merge is parked
# on the `plain_merge_task_pause_before_prepare` failpoint (see `05220_merge_memory_reservation_marks_streams`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local ratio="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05828_merge_memory_reservation_ratio_XXXXXX")

    local columns="k UInt64"
    local values="number"
    for i in $(seq 1 20); do
        columns="$columns, c$i UInt64"
        values="$values, number + $i"
    done

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_ratio ($columns)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, compress_marks = 0,
                 min_compress_block_size = 4194304, max_compress_block_size = 4194304,
                 min_columns_to_activate_adaptive_write_buffer = 0,
                 max_memory_ratio_to_activate_adaptive_write_buffer = ${ratio},
                 vertical_merge_algorithm_min_rows_to_activate = 1000000000,
                 ratio_of_defaults_for_sparse_serialization = 1;

        INSERT INTO t_merge_mem_ratio SELECT $values FROM numbers(100);
        INSERT INTO t_merge_mem_ratio SELECT $values FROM numbers(100, 100);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_ratio MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " -- --max_server_memory_usage=68719476736 < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

function reserved_for_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_merge "$1")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

without_ratio=$(reserved_for_merge_with_retries 0)
with_ratio=$(reserved_for_merge_with_retries 0.002)

# Both merges were selected and reserved before they parked on the failpoint.
echo "$((without_ratio > 0)) $((with_ratio > 0))"
# Without the trigger every stream holds two full 4 MiB buffers: over 160 MiB for 21 streams.
echo "$((without_ratio > 160000000))"
# With the trigger the streams are adaptive and the reservation is a small fraction of that.
echo "$((with_ratio * 10 < without_ratio))"
