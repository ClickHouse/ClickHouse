#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge into a part in `Packed` storage. `PackedFilesWriter` keeps the encoded contents of every file of the
# part until it is finalized, up to `max_bytes_to_buffer_for_packed_part` (0 means no limit), so the reservation
# has to grow with the data the merge writes, on top of what the same merge into `Full` storage reserves.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05402_merge_memory_reservation_vertical_readers).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local storage_settings="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05464_merge_memory_reservation_packed_archive_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_packed (k UInt64, v UInt64 CODEC(NONE))
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
                 min_rows_for_full_part_storage = 0, ${storage_settings},
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_packed SELECT number, number FROM numbers(1000000);
        INSERT INTO t_merge_mem_packed SELECT number, number FROM numbers(1000000, 1000000);
        INSERT INTO t_merge_mem_packed SELECT number, number FROM numbers(2000000, 1000000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_packed MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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
# The merge writes 48 MB of values (24 MB of them in the CODEC(NONE) column), all kept in the archive.
echo "$((packed - full >= 24000000))"
# With a spill budget the archive keeps at most that much.
echo "$((packed_with_spill - full == 1000000))"
