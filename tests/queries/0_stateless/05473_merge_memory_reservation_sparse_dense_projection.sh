#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) on the
# count-based adaptive write buffer rule for a REBUILT projection. The stream count of a rebuilt projection is
# taken from the source parts' streams, which is only an upper bound: a column that is sparse in one source part
# and dense in another records both `c` and `c.sparse.idx`, while writeTempProjectionPart recomputes the
# serialization from the merged block and writes the column densely, opening only `c`. The adaptive rule must be
# applied to the streams the projection writer is proven to open, not to that union.
#
# The table has a key and 40 payload columns: all zero (sparse) in one part and all nonzero (dense) in the other,
# so the source-stream union of the projection is 81 streams while its writer opens 41. With
# min_columns_to_activate_adaptive_write_buffer = 60 the writer allocates full-size eager buffers, exactly as with
# the rule disabled (0), so the two settings must reserve the same amount. Pricing the union would make the
# projection adaptive under 60 and reserve less. The projection overrides max_compress_block_size, so its eager
# buffers dominate the estimate.
#
# The reservation is observed as in 05219_merge_memory_reservation_adaptive_by_stream_count: a background merge
# parks on the plain_merge_task_pause_before_prepare failpoint after it has reserved its estimate, and the
# reserved amount is read from the MergesMutationsMemoryReservation metric. Each measurement runs in its own
# clickhouse-local process against its own data directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

columns=$(for i in $(seq 1 40); do echo -n ", c$i UInt64"; done)
names=$(for i in $(seq 1 40); do echo -n ", c$i"; done)
zeros=$(for i in $(seq 1 40); do echo -n ", 0"; done)
nonzeros=$(for i in $(seq 1 40); do echo -n ", number + $i"; done)

function reserved_for_merge()
{
    local min_columns_to_activate="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05473_merge_memory_reservation_sparse_dense_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_sparse_dense (k UInt64 ${columns})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0,
                 min_columns_to_activate_adaptive_write_buffer = ${min_columns_to_activate},
                 max_memory_ratio_to_activate_adaptive_write_buffer = 0,
                 ratio_of_defaults_for_sparse_serialization = 0.9375,
                 max_compress_block_size = 1048576, materialize_projections_on_merge = 1;

        -- Interleaved keys, so one rebuilt projection block holds the rows of both parts.
        INSERT INTO t_merge_mem_sparse_dense SELECT number * 2 ${zeros} FROM numbers(1000);
        INSERT INTO t_merge_mem_sparse_dense SELECT number * 2 + 1 ${nonzeros} FROM numbers(1000);

        -- Added after the inserts, so the merge rebuilds the projection from the merged rows.
        ALTER TABLE t_merge_mem_sparse_dense
            ADD PROJECTION p (SELECT k ${names} ORDER BY k) WITH SETTINGS (max_compress_block_size = 8388608);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_sparse_dense MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

threshold=$(reserved_for_merge_with_retries 60)
full_size=$(reserved_for_merge_with_retries 0)

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((threshold > 0))"
# The projection writer opens 41 streams, below the threshold of 60, so it is priced at full-size buffers.
echo "$((threshold == full_size))"
