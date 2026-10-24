#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# vertical merge whose output part crosses min_columns_to_activate_adaptive_write_buffer as a whole while its
# widest gathering column stays below it. A vertical merge takes the adaptive-write-buffer decision per
# writer: the writer of the Tuple column below opens its 49 streams with full-size buffers. The estimate
# used to cap the vertical price by the horizontal one, which takes the decision for the whole part (60
# streams, all adaptive) and so is much smaller.
#
# The check is an invariant: the gathering writers open the same full-size buffers with a threshold of 50
# as with a threshold that nothing reaches, so the reservation must not be smaller.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while the background merge is
# held on the plain_merge_task_pause_before_prepare failpoint, as in 05220_merge_memory_reservation_marks_streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

tuple_type="Tuple($(for i in $(seq 1 49); do printf 'e%d UInt8' "$i"; [ "$i" -lt 49 ] && printf ', '; done))"
tuple_value="tuple($(for i in $(seq 1 49); do printf 'toUInt8(number + %d)' "$i"; [ "$i" -lt 49 ] && printf ', '; done))"
scalar_columns="$(for i in $(seq 1 10); do printf ', c%d UInt64' "$i"; done)"
scalar_values="$(for i in $(seq 1 10); do printf ', number * %d' "$i"; done)"

function reserved_for_merge()
{
    local min_columns_to_activate_adaptive_write_buffer="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05406_merge_memory_reservation_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_vertical_adaptive (k UInt64, t ${tuple_type} ${scalar_columns})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 1,
                 vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_bytes_to_activate = 0,
                 vertical_merge_algorithm_min_columns_to_activate = 1,
                 min_columns_to_activate_adaptive_write_buffer = ${min_columns_to_activate_adaptive_write_buffer};

        INSERT INTO t_merge_mem_vertical_adaptive SELECT number, ${tuple_value} ${scalar_values} FROM numbers(1000);
        INSERT INTO t_merge_mem_vertical_adaptive SELECT number, ${tuple_value} ${scalar_values} FROM numbers(1000, 1000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_vertical_adaptive MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

whole_part_adaptive=$(reserved_for_merge_with_retries 50)
never_adaptive=$(reserved_for_merge_with_retries 1000)

echo "$((whole_part_adaptive > 0))"
echo "$((whole_part_adaptive >= never_adaptive))"
