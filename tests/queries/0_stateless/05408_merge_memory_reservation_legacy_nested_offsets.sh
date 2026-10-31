#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) on
# a legacy wide part with a Nested layout. A wide part written before columns_substreams.txt existed is priced
# by its .bin files on disk. With share_nested_offsets the columns of one Nested object share a single offsets
# file (n.size0.bin), named after the Nested object rather than after any of its columns, so a filter keeping
# only the files that start with a column's escaped name dropped it: the legacy part was priced one reader
# buffer pair short per Nested object.
#
# The check is an invariant: the same data must reserve the same amount whether its parts record their
# substreams in columns_substreams.txt or are legacy parts without that file.
#
# The reservation itself is the observable: a background merge is held on the
# plain_merge_task_pause_before_prepare failpoint right after StorageMergeTree::selectPartsToMerge has
# reserved its estimate, and the reserved amount is read from the MergesMutationsMemoryReservation metric
# while it waits. Each measurement runs in its own clickhouse-local process against its own data directory,
# so the process-wide metric only ever reflects that one merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the largest reservation observed while the selected merge is parked on the failpoint.
# With the argument "legacy", columns_substreams.txt is deleted from every part before the merge.
#
# The data is written by one clickhouse-local process and merged by the next one: a background merge is
# only selected once min_age_to_force_merge_seconds is set, and the second process sets it only after it has
# armed the failpoint, so the merge cannot be selected (and run to completion unobserved) before that.
function reserved_for_merge()
{
    local layout="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05408_merge_memory_reservation_legacy_nested_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_legacy_nested (k UInt64, n Nested(a UInt64, b UInt64, c UInt64, d UInt64))
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1;

        INSERT INTO t_merge_mem_legacy_nested SELECT number, arr, arr, arr, arr FROM (SELECT number, range(number % 7) AS arr FROM numbers(1000));
        INSERT INTO t_merge_mem_legacy_nested SELECT number, arr, arr, arr, arr FROM (SELECT number, range(number % 7) AS arr FROM numbers(1000, 1000));
        INSERT INTO t_merge_mem_legacy_nested SELECT number, arr, arr, arr, arr FROM (SELECT number, range(number % 7) AS arr FROM numbers(2000, 1000));
    " < /dev/null

    if [ "$layout" = "legacy" ]; then
        find "$data_dir" -type f -name 'columns_substreams.txt' -delete
    fi

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;

        ALTER TABLE t_merge_mem_legacy_nested MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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
legacy=$(reserved_for_merge_with_retries "legacy")

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((recorded > 0))"
# The legacy part's shared Nested offsets are priced like the recorded ones.
if [ "$recorded" -eq "$legacy" ]; then echo 1; else echo "recorded ${recorded} legacy ${legacy}"; fi
