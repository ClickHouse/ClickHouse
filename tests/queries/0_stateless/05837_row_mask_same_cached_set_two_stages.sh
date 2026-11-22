#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

run_case()
{
    local label=$1
    local enabled=$2
    local table="row_mask_same_cached_set_${label}"

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};
        CREATE TABLE ${table}
        (
            tenant Int32,
            item_key UInt128,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree ORDER BY (tenant, item_key)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            enable_row_mask_typed_set_union = ${enabled},
            max_row_mask_update_coalescing_commands = 4096,
            max_row_mask_update_coalescing_ast_bytes = 1048576;

        INSERT INTO ${table}
        SELECT toInt32(1), toUInt128(item_key), initializeAggregation('sumState', value)
        FROM values('item_key UInt8, value Int64', (1, 10), (2, 20), (3, 30), (4, 40));

        ALTER TABLE ${table} UPDATE _row_exists = 0 WHERE item_key = toUInt128(3)
        SETTINGS mutations_sync = 2;
    "

    local mutation_start
    mutation_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")

    # The two commands retain separate stages and the same `Set` cache key.
    # `enable_sharing_sets_for_mutations` is ON by default in the background context.
    $CLICKHOUSE_CLIENT --query "
        ALTER TABLE ${table}
            UPDATE _row_exists = 0 WHERE (tenant, item_key) IN
                (SELECT arrayJoin(arrayZip([toInt32(1), toInt32(1)], [toUInt128(2), toUInt128(4)]))),
            UPDATE _row_exists = 0 WHERE (tenant, item_key) IN
                (SELECT arrayJoin(arrayZip([toInt32(1), toInt32(1)], [toUInt128(2), toUInt128(4)])))
        SETTINGS mutations_sync = 2;
    "

    local log_filter="
        FROM system.part_log
        WHERE event_date >= yesterday()
            AND toUnixTimestamp64Micro(event_time_microseconds) >= ${mutation_start}
            AND database = currentDatabase() AND table = '${table}'
            AND table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '${table}')
            AND event_type = 'MutatePart' AND error = 0
    "

    # `mutations_sync` can return before the `MutatePart` entry reaches `part_log`.
    local deadline=$((SECONDS + 60))
    local poll_seconds
    local logged=0
    while (( SECONDS < deadline ))
    do
        poll_seconds=$((deadline - SECONDS))
        (( poll_seconds > 0 )) || break
        timeout --foreground -k 1 "$poll_seconds" $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log" >/dev/null
        poll_seconds=$((deadline - SECONDS))
        (( poll_seconds > 0 )) || break
        logged=$(timeout --foreground -k 1 "$poll_seconds" $CLICKHOUSE_CLIENT --max_execution_time 10 --query "
            SELECT count() > 0 ${log_filter}
        ")
        [[ "$logged" == 1 ]] && break
        sleep 0.1
    done
    if [[ "$logged" != 1 ]]
    then
        echo "Timed out waiting for mutation part log of ${table}" >&2
        return 1
    fi

    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT '${label}_dispatch', count() = 1 AND max(length(mutation_ids)) = 1
            AND sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${enabled}
        ${log_filter};

        SELECT '${label}_physical', item_key, _row_exists, finalizeAggregation(s)
        FROM ${table} ORDER BY item_key
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_visible', item_key, sumMerge(s)
        FROM ${table} GROUP BY item_key ORDER BY item_key
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table};
    "
}

run_case off 0
run_case on 1
