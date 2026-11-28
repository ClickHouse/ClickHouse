#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# readers of a vertical merge that fills a missing column from its default expression. The reader of that
# gathering column also reads every column the expression depends on (`injectRequiredColumns`), so the
# single-column reader sets of the vertical pricing do not bound it, and the horizontal reader set is kept.
# A default that depends on forty columns must therefore reserve more than a constant default.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05402_merge_memory_reservation_vertical_readers).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local default_expression="$1"
    local column_definitions column_values data_dir
    column_definitions=$(seq -s ', ' -f 'c%g UInt64' 1 40)
    column_values=$(seq -s ', ' -f 'number * %g' 1 40)
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05463_merge_memory_reservation_vertical_default_dependencies_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_default (k UInt64, ${column_definitions})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_bytes_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_default SELECT number, ${column_values} FROM numbers(10000);
        INSERT INTO t_merge_mem_default SELECT number, ${column_values} FROM numbers(10000, 10000);
        INSERT INTO t_merge_mem_default SELECT number, ${column_values} FROM numbers(20000, 10000);

        ALTER TABLE t_merge_mem_default ADD COLUMN d UInt64 DEFAULT ${default_expression};
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_default MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

constant_default=$(reserved_for_merge_with_retries "0")
dependent_default=$(reserved_for_merge_with_retries "sipHash64($(seq -s ', ' -f 'c%g' 1 40))")

echo "$((constant_default > 0))"
echo "$((dependent_default > constant_default))"
