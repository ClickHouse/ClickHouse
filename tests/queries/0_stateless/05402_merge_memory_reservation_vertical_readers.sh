#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# readers of a vertical merge. A vertical merge reads the merging columns in its horizontal stage and then
# reads one gathering column at a time, so on a local disk neither its readers nor its writers depend on the
# number of gathering columns. Twice as many identical gathering columns must therefore reserve exactly as
# much. Pricing the readers of every column as concurrent would grow the reservation with the column count.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local columns="$1"
    local column_definitions column_values data_dir
    column_definitions=$(seq -s ', ' -f 'c%g UInt64' 1 "$columns")
    column_values=$(seq -s ', ' -f 'number + 0 * %g' 1 "$columns")
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05402_merge_memory_reservation_vertical_readers_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_vertical (k UInt64, ${column_definitions})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_bytes_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_vertical SELECT number, ${column_values} FROM numbers(10000);
        INSERT INTO t_merge_mem_vertical SELECT number, ${column_values} FROM numbers(10000, 10000);
        INSERT INTO t_merge_mem_vertical SELECT number, ${column_values} FROM numbers(20000, 10000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_vertical MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

twenty_columns=$(reserved_for_merge_with_retries 20)
forty_columns=$(reserved_for_merge_with_retries 40)

echo "$((twenty_columns > 0))"
echo "$((forty_columns == twenty_columns))"
