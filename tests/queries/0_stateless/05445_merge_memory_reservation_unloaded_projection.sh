#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# projection that the source parts have on disk but not loaded. That happens when a replica fetches parts
# before it applies the ALTER that adds their projection (05315_projection_unloaded_after_fetch_rebuilt).
# MergeTask::prepareProjectionsToMergeAndRebuild rebuilds such a projection even with the default
# materialize_projections_on_merge = 0, so the reservation must price the rebuild. Here the parts get an
# unloaded projection by removing it from the table metadata between two clickhouse-local runs and adding it
# back with ALTER. The merge of the same parts without the projection directories on disk drops the
# projection and must reserve less.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COLUMNS=20
COLUMN_DEFINITIONS=$(seq -s ', ' -f 'c%g UInt64' 1 "$COLUMNS")
COLUMN_VALUES=$(seq -s ', ' -f 'number * %g' 1 "$COLUMNS")
PROJECTION="PROJECTION p (SELECT * ORDER BY c2)"

function reserved_for_merge()
{
    local projection_on_disk="$1"
    local data_dir projection_clause
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05445_merge_memory_reservation_unloaded_projection_XXXXXX")
    projection_clause=""
    if [ "$projection_on_disk" = 1 ]; then projection_clause=", ${PROJECTION}"; fi

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_unloaded (k UInt64, ${COLUMN_DEFINITIONS}${projection_clause})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_projections_on_merge = 0,
                 enable_vertical_merge_algorithm = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_unloaded SELECT number, ${COLUMN_VALUES} FROM numbers(1000);
        INSERT INTO t_merge_mem_unloaded SELECT number, ${COLUMN_VALUES} FROM numbers(1000, 1000);
    " < /dev/null

    # Remove the projection from the table metadata, keeping its directories in the parts.
    local metadata_file
    metadata_file=$(find "$data_dir" -name 't_merge_mem_unloaded.sql' | head -1)
    perl -0pi -e 's/,\s*PROJECTION p\s*\([^()]*\)//s' "$metadata_file"

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        ALTER TABLE t_merge_mem_unloaded ADD ${PROJECTION};
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_unloaded MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

unloaded_projection=$(reserved_for_merge_with_retries 1)
no_projection=$(reserved_for_merge_with_retries 0)

echo "$((no_projection > 0))"
echo "$((unloaded_projection > no_projection))"
