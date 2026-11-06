#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge into a part in `Packed` storage that rebuilds a projection. A packed part gives every projection its own
# packed storage with its own `PackedFilesWriter`, which keeps the encoded contents of the projection's files on
# top of the parent's archive, up to `max_bytes_to_buffer_for_packed_part` (0 means no limit). So the reservation
# of such a merge has to include a second archive, sized by the rebuilt projection, not only the parent's one
# (see 05464_merge_memory_reservation_packed_archive for the parent archive).
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local storage_settings="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05476_merge_memory_reservation_packed_projection_archive_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_packed_proj (k UInt64, v UInt64 CODEC(NONE))
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
                 min_rows_for_full_part_storage = 0, ${storage_settings},
                 materialize_projections_on_merge = 1,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_packed_proj SELECT number, number FROM numbers(1000000);
        INSERT INTO t_merge_mem_packed_proj SELECT number, number FROM numbers(1000000, 1000000);
        INSERT INTO t_merge_mem_packed_proj SELECT number, number FROM numbers(2000000, 1000000);

        -- Added after the inserts, so no source part has it and the merge rebuilds it.
        ALTER TABLE t_merge_mem_packed_proj ADD PROJECTION p (SELECT k, v ORDER BY v);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_packed_proj MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

full=$(reserved_for_merge_with_retries "min_bytes_for_full_part_storage = 0")
packed=$(reserved_for_merge_with_retries "min_bytes_for_full_part_storage = 1000000000")
packed_with_spill=$(reserved_for_merge_with_retries "min_bytes_for_full_part_storage = 1000000000, max_bytes_to_buffer_for_packed_part = 1000000")

echo "$((full > 0))"
# The merge writes 48 MB of values into the parent's archive and the same 48 MB again into the rebuilt
# projection's archive.
echo "$((packed - full >= 96000000))"
# With a spill budget each of the two archives keeps at most that much.
echo "$((packed_with_spill - full == 2000000))"
