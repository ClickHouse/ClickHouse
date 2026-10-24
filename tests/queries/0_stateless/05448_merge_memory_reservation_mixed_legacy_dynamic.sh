#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge that mixes modern parts with a legacy wide part written before columns_substreams.txt existed. The
# legacy part's dynamic substreams of `JSON` / `Dynamic` columns are recovered from its .bin files and must be
# unioned by name with the substreams the modern parts record: a stream both kinds of parts store is written
# once by the merged part, so it must not be charged twice.
#
# The check is an invariant: identical parts (the same JSON paths and Dynamic types) must reserve the same
# amount whether all of them record their substreams, one of them is a legacy part without that file, or all
# of them are. This also covers the bytes a legacy part reads: its JSON path files are not part of the column
# size the part computes from its default serialization, and must still be priced.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while the selected merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05408_merge_memory_reservation_legacy_nested_offsets).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the largest reservation observed while the selected merge is parked on the failpoint.
# With the argument "mixed", columns_substreams.txt is deleted from one part before the merge, with "legacy"
# from every part.
function reserved_for_merge()
{
    local layout="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05448_merge_memory_reservation_mixed_legacy_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_mixed_legacy (k UInt64, j JSON, d Dynamic)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

        INSERT INTO t_merge_mem_mixed_legacy SELECT number, toJSONString(map('a', number, 'b', toString(number), 'c', number % 3)), if(number % 2, number::Dynamic, toString(number)::Dynamic) FROM numbers(1000);
        INSERT INTO t_merge_mem_mixed_legacy SELECT number, toJSONString(map('a', number, 'b', toString(number), 'c', number % 3)), if(number % 2, number::Dynamic, toString(number)::Dynamic) FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_mixed_legacy SELECT number, toJSONString(map('a', number, 'b', toString(number), 'c', number % 3)), if(number % 2, number::Dynamic, toString(number)::Dynamic) FROM numbers(2000, 1000);
    " < /dev/null

    if [ "$layout" = "mixed" ]; then
        find "$data_dir" -type f -name 'columns_substreams.txt' | sort | head -1 | xargs rm -f
    elif [ "$layout" = "legacy" ]; then
        find "$data_dir" -type f -name 'columns_substreams.txt' -delete
    fi

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;

        ALTER TABLE t_merge_mem_mixed_legacy MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';

        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

# A zero (empty) measurement means the background selector did not pick the merge before the timeout at all -
# retry the whole measurement on fresh data.
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

recorded=$(reserved_for_merge_with_retries "recorded")
mixed=$(reserved_for_merge_with_retries "mixed")
legacy=$(reserved_for_merge_with_retries "legacy")

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((recorded > 0))"
# The legacy part's dynamic streams that the modern parts also record are not charged twice.
if [ "$recorded" -eq "$mixed" ]; then echo 1; else echo "recorded ${recorded} mixed ${mixed}"; fi
# Legacy parts are priced like the recorded ones, including the bytes of their JSON path files.
if [ "$recorded" -eq "$legacy" ]; then echo 1; else echo "recorded ${recorded} legacy ${legacy}"; fi
