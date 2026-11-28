#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge that reads `String` columns stored with sparse serialization and writes them densely. A sparse column
# stores only its offsets and non-default values, so the empty strings take no space in the source parts, but a
# dense writer emits the serialized default of every row (a length byte or a `UInt64` size per empty string).
# Unlike a fixed-size type, `String` has no maximum value size, so the densified rows are priced by the size of
# the type default (densifiedDefaultValueSize).
#
# The check is an invariant, as in 05616_merge_memory_reservation_sparse_to_dense: the same sparse source parts
# must reserve strictly more when ratio_of_defaults_for_sparse_serialization is raised to 1 before the merge
# than when the merged part stays sparse. A background merge is held on the plain_merge_task_pause_before_prepare
# failpoint right after it reserved its estimate, and the amount is read from the MergesMutationsMemoryReservation
# metric while it waits. Each measurement runs in its own clickhouse-local process against its own data directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local merge_ratio="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05768_merge_memory_reservation_sparse_string_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_sparse_to_dense
        (
            k UInt64,
            c0 String, c1 String, c2 String, c3 String, c4 String,
            c5 String, c6 String, c7 String, c8 String, c9 String
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, skip_empty_columns_on_insert = 0,
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
