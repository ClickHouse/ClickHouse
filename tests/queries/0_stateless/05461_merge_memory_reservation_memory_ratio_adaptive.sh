#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) and
# the memory-ratio trigger of adaptive write buffers. MergeTreeDataPartWriterWide::addStreams starts every
# stream with an adaptive (small) buffer when the eager buffers of all its streams
# (2 * max_compress_block_size each) would take at least max_memory_ratio_to_activate_adaptive_write_buffer
# of the server memory limit. The estimate must take the same decision: with an 8 GiB limit and a ratio of
# 1/64, 21 streams of 4 MiB blocks (168 MiB >= 128 MiB) are adaptive, so a merge of tiny parts must reserve
# much less than with the trigger disabled, where every stream eagerly allocates its full-size buffers. The
# limit is far above the RSS of clickhouse-local, so it never rejects the queries themselves.
#
# The reservation is observed the same way as in 05218_merge_memory_reservation_nested_shared_offsets: a
# background merge is held on the plain_merge_task_pause_before_prepare failpoint after it has reserved its
# estimate, and the MergesMutationsMemoryReservation metric is read while it waits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local ratio="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05461_merge_memory_reservation_ratio_XXXXXX")

    local columns="k UInt64"
    local values="number"
    for i in $(seq 1 20)
    do
        columns="${columns}, c${i} UInt64"
        values="${values}, number + ${i}"
    done

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_ratio (${columns})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, max_compress_block_size = 4194304,
            min_columns_to_activate_adaptive_write_buffer = 0, max_memory_ratio_to_activate_adaptive_write_buffer = ${ratio};

        INSERT INTO t_merge_mem_ratio SELECT ${values} FROM numbers(2);
        INSERT INTO t_merge_mem_ratio SELECT ${values} FROM numbers(2, 2);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_ratio MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " -- --max_server_memory_usage=8589934592 < /dev/null | sort -rn | head -1

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

adaptive=$(reserved_for_merge_with_retries 0.015625)
eager=$(reserved_for_merge_with_retries 0)

echo "$((adaptive > 0))"
# The adaptive streams are not priced at their full-size eager buffers.
echo "$((2 * adaptive < eager))"
