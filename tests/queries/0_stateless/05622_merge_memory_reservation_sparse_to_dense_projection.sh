#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge that rebuilds a projection over columns stored with sparse serialization. The rebuilt projection chooses
# its serialization kind with its own ratio_of_defaults_for_sparse_serialization (WITH SETTINGS), so it can write
# densely while the merged base part stays sparse. The uncompressed size of a sparse source column counts only its
# offsets and non-default values, so the estimate must price the densified rows of the projection as well.
#
# The check is an invariant: the same source parts and the same base settings must reserve strictly more when
# the projection writes its columns densely than when it keeps them sparse. The reservation is read from the
# MergesMutationsMemoryReservation metric while the merge is held on a failpoint, as in
# 05616_merge_memory_reservation_sparse_to_dense.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local projection_ratio="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05622_merge_memory_reservation_sparse_projection_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_sparse_to_dense_projection
        (
            k UInt64,
            c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64,
            c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
                 vertical_merge_algorithm_min_rows_to_activate = 1000000000,
                 min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 16384,
                 materialize_projections_on_merge = 1;

        INSERT INTO t_sparse_to_dense_projection (k) SELECT number FROM numbers(200000);
        INSERT INTO t_sparse_to_dense_projection (k) SELECT number FROM numbers(200000, 200000);

        -- Added after the inserts and not materialized: the source parts have no projection parts, so the merge
        -- rebuilds it from the merged rows.
        ALTER TABLE t_sparse_to_dense_projection ADD PROJECTION p (SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9 ORDER BY c0)
            WITH SETTINGS (min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = ${projection_ratio});
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_sparse_to_dense_projection MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

stays_sparse=$(reserved_for_merge_with_retries 0.9)
becomes_dense=$(reserved_for_merge_with_retries 1)

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((stays_sparse > 0))"
# The default rows the rebuilt projection writes densely are part of the reservation.
echo "$((becomes_dense > stays_sparse))"
