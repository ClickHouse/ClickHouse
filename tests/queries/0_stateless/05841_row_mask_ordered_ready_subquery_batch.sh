#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

run_case()
{
    local label=$1
    local enabled=$2
    local table="ready_ordered_${label}"

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};

        CREATE TABLE ${table}
        (
            n UInt8,
            guard UInt8,
            item_key UInt64,
            alternate_key UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree ORDER BY (guard, item_key, alternate_key, n)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0,
            enable_row_mask_action_batching = ${enabled}, enable_row_mask_typed_set_union = 1;

        INSERT INTO ${table}
        SELECT n, guard, item_key, alternate_key, initializeAggregation('sumState', seed_value)
        FROM values('n UInt8, guard UInt8, item_key UInt64, alternate_key UInt64, seed_value Int64',
            (0, 1, 0, 10, 10), (1, 0, 2, 20, 20), (2, 1, 2, 20, 30),
            (3, 1, 3, 30, 40), (4, 1, 4, 40, 50), (5, 0, 4, 40, 60),
            (6, 1, 5, 50, 70));

        ALTER TABLE ${table} UPDATE _row_exists = 0 WHERE n = 6 SETTINGS mutations_sync = 2;
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE guard = toUInt8(1) AND item_key IN
        (
            SELECT if(number = 0, toUInt64(0), toUInt64(2)) FROM numbers(2)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        DELETE FROM ${table} WHERE guard = toUInt8(0) AND alternate_key IN
        (
            SELECT if(number = 0, toUInt64(20), toUInt64(40)) FROM numbers(2)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
    "

    local queued_ids
    queued_ids=$($CLICKHOUSE_CLIENT --query "
        SELECT arraySort(groupArray(mutation_id))
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done
        HAVING count() = 2 AND countIf(position(command, '_row_exists') > 0) = 2
    ")
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"
    if [[ -z "$queued_ids" ]]
    then
        echo "Expected two queued row-mask mutations for ${table}" >&2
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
            sum(ProfileEvents['RowMaskActionBatches']) = ${enabled},
            sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = 0,
            sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0
        ${log_filter};

        SELECT '${label}_mask', n, guard, item_key, alternate_key, _row_exists, finalizeAggregation(s)
        FROM ${table} ORDER BY n
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_visible', count() = 2, sumMerge(s) = 90
        FROM ${table}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table};
    "
}

run_case off 0
run_case on 1
