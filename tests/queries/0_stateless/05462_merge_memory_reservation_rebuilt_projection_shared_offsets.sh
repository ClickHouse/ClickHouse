#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# REBUILT projection over a Nested object. With share_nested_offsets the columns n.a and n.b share one offsets
# stream (n.size0), which the wide writer of the temporary projection part opens once: three streams, not
# four. The count-based adaptive write buffer rule compares exactly that deduplicated count with
# min_columns_to_activate_adaptive_write_buffer, so with a threshold of 4 the projection writer allocates
# full-size eager buffers, and with a threshold of 3 it starts every stream with a small adaptive buffer.
# The base part has four streams (k, n.a, n.b, n.size0), so it is adaptive under both thresholds and only
# the projection makes the difference. Counting the shared offsets once per sibling column would make the
# projection adaptive under both thresholds and reserve the same amount for both.
#
# The reservation is observed the same way as in 05218_merge_memory_reservation_nested_shared_offsets: a
# background merge is held on the plain_merge_task_pause_before_prepare failpoint after it has reserved its
# estimate, and the MergesMutationsMemoryReservation metric is read while it waits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local adaptive_threshold="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05462_merge_memory_reservation_projection_XXXXXX")

    # The projection is added after the parts are written and is not materialized, so the merge rebuilds it
    # (materialize_projections_on_merge).
    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_nested_projection (k UInt64, n Nested(a UInt64, b UInt64))
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, share_nested_offsets = 1, materialize_projections_on_merge = 1,
            min_columns_to_activate_adaptive_write_buffer = ${adaptive_threshold},
            max_memory_ratio_to_activate_adaptive_write_buffer = 0;

        INSERT INTO t_merge_mem_nested_projection SELECT number, range(number % 3), range(number % 3) FROM numbers(2);
        INSERT INTO t_merge_mem_nested_projection SELECT number, range(number % 3), range(number % 3) FROM numbers(2, 2);

        ALTER TABLE t_merge_mem_nested_projection ADD PROJECTION p (SELECT n.a, n.b ORDER BY n.a);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_nested_projection MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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
        result=$(reserved_for_merge "$1")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

adaptive=$(reserved_for_merge_with_retries 3)
eager=$(reserved_for_merge_with_retries 4)

echo "$((adaptive > 0))"
# The projection writer opens three streams, below the threshold of 4: its eager buffers are full-size.
echo "$((adaptive < eager))"
