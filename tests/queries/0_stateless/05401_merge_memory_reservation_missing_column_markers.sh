#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# column that no source part stores but the merge materializes. With skip_empty_columns_on_insert = 1 and
# serialization_info_version = 'with_missing_columns' a part that omits an all-default column records a
# missing-column marker instead. MergeTask writes such a column unless every non-empty source part carries a
# marker for it, so a merge of a part written before ALTER ... ADD COLUMN (no marker) and a part written after
# it with only default values (a marker) writes the column. The estimate must price its writer streams, not
# expire it like a column that no source part knows about.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local inserts="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05401_merge_memory_reservation_missing_column_markers_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_markers (k UInt64, s String)
        ENGINE = MergeTree ORDER BY k
        SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 0, enable_block_number_column = 0, enable_block_offset_column = 0;
        ${inserts}
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_markers MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

# Both parts are written before the column is added: no part knows it, and the merge expires it.
expired=$(reserved_for_merge_with_retries "
    INSERT INTO t_merge_mem_markers SELECT number, toString(number) FROM numbers(1000);
    INSERT INTO t_merge_mem_markers SELECT number, toString(number) FROM numbers(1000, 1000);
    ALTER TABLE t_merge_mem_markers ADD COLUMN b UInt64;")

# The second part is written after the column is added and records a marker for it: the merge writes it.
materialized=$(reserved_for_merge_with_retries "
    INSERT INTO t_merge_mem_markers SELECT number, toString(number) FROM numbers(1000);
    ALTER TABLE t_merge_mem_markers ADD COLUMN b UInt64;
    INSERT INTO t_merge_mem_markers SELECT number, toString(number), 0 FROM numbers(1000, 1000);")

echo "$((expired > 0))"
echo "$((materialized > expired))"
