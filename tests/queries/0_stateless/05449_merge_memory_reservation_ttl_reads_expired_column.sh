#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a TTL
# merge that `MergeTask::chooseMergeAlgorithm` forces to Horizontal. The rows TTL reads a column that no source
# part stores and that has no default, so the merge marks it expired, and `MergeTask::canVerticalTTLDelete`
# rejects the vertical merge: only `TTLTransform`, after the merge, fills such a column. The merge then opens
# the writer streams of all payload columns at once, each with a full compressor and file buffer (1 MiB each,
# the table has fewer columns than `min_columns_to_activate_adaptive_write_buffer`). The estimate priced it as
# a vertical merge, which keeps the streams of the merging columns and a single gathering column alive.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COLUMNS=40

function reserved_for_merge()
{
    local column_definitions column_values data_dir
    column_definitions=$(seq -s ', ' -f 'c%g UInt64' 1 "$COLUMNS")
    column_values=$(seq -s ', ' -f 'number * %g' 1 "$COLUMNS")
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05449_merge_memory_reservation_ttl_reads_expired_column_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_ttl (k UInt64, ${column_definitions})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_bytes_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
                 vertical_merge_optimize_ttl_delete = 1, min_columns_to_activate_adaptive_write_buffer = 500,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_ttl SELECT number, ${column_values} FROM numbers(1000);
        INSERT INTO t_merge_mem_ttl SELECT number, ${column_values} FROM numbers(1000, 1000);

        ALTER TABLE t_merge_mem_ttl ADD COLUMN d DateTime;
        ALTER TABLE t_merge_mem_ttl MODIFY TTL d + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_ttl MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

result=0
for _ in 1 2 3
do
    result=$(reserved_for_merge)
    result=${result:-0}
    if [ "$result" -gt 0 ]; then break; fi
done

echo "$((result > 0))"
# A compressor and a file buffer of 1 MiB = 1048576 bytes for every payload column.
echo "$((result >= COLUMNS * 2 * 1048576))"
