#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge that reads columns stored with sparse serialization and writes them densely. The uncompressed size of
# a sparse column on disk counts only its offsets and non-default values, so it cannot bound the bytes a dense
# writer emits for the same rows: every default becomes a full value. The estimate must price those rows
# (countRowsDensifiedFromSparse).
#
# The check is an invariant: the same sparse source parts must reserve strictly more when
# ratio_of_defaults_for_sparse_serialization is raised to 1 before the merge (MergeTask then writes every
# column densely) than when the merged part stays sparse. Without the densified rows both reservations are
# equal, because the source parts and their streams are the same.
#
# The reservation itself is the observable, as in 05220_merge_memory_reservation_marks_streams: a background
# merge is held on the plain_merge_task_pause_before_prepare failpoint right after it reserved its estimate,
# and the amount is read from the MergesMutationsMemoryReservation metric while it waits. Each measurement
# runs in its own clickhouse-local process against its own data directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local merge_ratio="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05616_merge_memory_reservation_sparse_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_sparse_to_dense
        (
            k UInt64,
            c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64,
            c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
                 vertical_merge_algorithm_min_rows_to_activate = 1000000000;

        INSERT INTO t_sparse_to_dense (k) SELECT number FROM numbers(200000);
        INSERT INTO t_sparse_to_dense (k) SELECT number FROM numbers(200000, 200000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_sparse_to_dense MODIFY SETTING ratio_of_defaults_for_sparse_serialization = ${merge_ratio};
        ALTER TABLE t_sparse_to_dense MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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
# The default rows written densely are part of the reservation.
echo "$((becomes_dense > stays_sparse))"
