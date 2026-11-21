#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

run_case()
{
    local label=$1
    local enabled=$2
    local source_rows=$3
    local source_key_bytes=$4
    local groups=$5
    local expected_ready_batches=$6
    local third_key=${7:-b}
    local third_value=13
    if [[ "$third_key" == a ]]
    then
        third_value=6000
    fi
    local table="ready_set_limits_${label}"

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};

        CREATE TABLE ${table}
        (
            a UInt64,
            b UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree ORDER BY (a, b)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            enable_row_mask_typed_set_union = ${enabled},
            max_row_mask_typed_set_union_source_rows = ${source_rows},
            max_row_mask_typed_set_union_source_key_bytes = ${source_key_bytes},
            max_row_mask_typed_set_union_groups = ${groups};

        INSERT INTO ${table}
        SELECT a, b, initializeAggregation('sumState', value)
        FROM values('a UInt64, b UInt64, value Int64',
            (0, 10, 10), (1, 11, 20), (5000, 12, 30),
            (6000, 13, 40), (7000, 14, 50), (8000, 15, 60));

        ALTER TABLE ${table} UPDATE _row_exists = 0 WHERE a = 7000 SETTINGS mutations_sync = 2;
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE a IN (SELECT number FROM numbers(4097))
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        DELETE FROM ${table} WHERE a IN (SELECT toUInt64(5000) FROM numbers(1))
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        DELETE FROM ${table} WHERE ${third_key} IN (SELECT toUInt64(${third_value}) FROM numbers(1))
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
    "

    local queued_ids
    queued_ids=$($CLICKHOUSE_CLIENT --query "
        SELECT arraySort(groupArray(mutation_id))
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done
        HAVING count() = 3 AND countIf(position(command, '_row_exists') > 0) = 3
    ")
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"
    if [[ -z "$queued_ids" ]]
    then
        echo "Expected three queued row-mask mutations for ${table}" >&2
        return 1
    fi

    local log_filter="
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
          AND database = currentDatabase() AND table = '${table}'
          AND table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '${table}')
          AND event_type = 'MutatePart' AND error = 0 AND hasAny(mutation_ids, ${queued_ids})
    "
    local deadline=$((SECONDS + 60))
    local logged=0
    while (( SECONDS < deadline ))
    do
        logged=$(timeout --foreground -k 1 "$((deadline - SECONDS))" $CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
            SYSTEM FLUSH LOGS part_log;
            SELECT arraySort(arrayDistinct(arrayFlatten(groupArray(mutation_ids)))) = ${queued_ids}
            ${log_filter}
        ")
        [[ "$logged" == 1 ]] && break
        sleep 0.1
    done
    if [[ "$logged" != 1 ]]
    then
        echo "Timed out waiting for completed mutations for ${table}" >&2
        return 1
    fi

    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT '${label}_dispatch', count() = 1 AND arraySort(any(mutation_ids)) = ${queued_ids},
            sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${expected_ready_batches},
            sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0
        ${log_filter};

        SELECT '${label}_mask', a, b, _row_exists, finalizeAggregation(s)
        FROM ${table} ORDER BY a
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_visible', count() = 1, sumMerge(s) = 60
        FROM ${table}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table};
    "
}

run_case off 0 5000 65536 2 0
run_case default_rows 1 4096 65536 2 0
run_case raised_rows 1 5000 65536 2 1
run_case low_bytes 1 5000 1024 2 0
run_case one_group 1 5000 65536 1 0
run_case zero_rows 1 0 65536 2 0
run_case single_raised 1 5000 65536 1 1 a
run_case single_low_bytes 1 5000 32784 1 0 a
