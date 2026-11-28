#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# nested merge of an aggregate projection. MergeProjectionsStage::prepareProjections merges an aggregate
# projection with the Aggregating mode whatever the table's mode is, and an Aggregating merge is never
# vertical, so all of its columns are written at once. The estimate used to price that nested merge with
# the table's Ordinary mode, as a vertical merge that writes one gathering column at a time.
#
# The check compares an aggregate projection with a normal projection of the same 64 columns: under the
# low vertical merge thresholds below the normal one is merged vertically, the aggregate one horizontally,
# with all 64 writer streams alive at once. Each of the 63 extra streams holds a 1 MiB compressor block and
# a 1 MiB file buffer, so the aggregate reservation is larger by well over 64 MiB. The base merge is priced
# the same in both cases.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while the background merge is
# held on the plain_merge_task_pause_before_prepare failpoint, as in 05220_merge_memory_reservation_marks_streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

value_columns="$(for i in $(seq 1 64); do printf ', v%d UInt64' "$i"; done)"
values="$(for i in $(seq 1 64); do printf ', number * %d' "$i"; done)"
aggregate_projection="SELECT k$(for i in $(seq 1 64); do printf ', sum(v%d)' "$i"; done) GROUP BY k"
normal_projection="SELECT k$(for i in $(seq 1 64); do printf ', v%d' "$i"; done) ORDER BY k"

function reserved_for_merge()
{
    local projection="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05407_merge_memory_reservation_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_aggregate_projection (k UInt64, s String ${value_columns}, PROJECTION p (${projection}))
        ENGINE = MergeTree ORDER BY s
        SETTINGS min_bytes_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 1,
                 vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_bytes_to_activate = 0,
                 vertical_merge_algorithm_min_columns_to_activate = 1;

        INSERT INTO t_merge_mem_aggregate_projection SELECT number, toString(number) ${values} FROM numbers(1000);
        INSERT INTO t_merge_mem_aggregate_projection SELECT number, toString(number) ${values} FROM numbers(1000, 1000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_aggregate_projection MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

aggregate=$(reserved_for_merge_with_retries "$aggregate_projection")
normal=$(reserved_for_merge_with_retries "$normal_projection")

echo "$((normal > 0))"
echo "$((aggregate - normal > 64 * 1048576))"
