#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

run_case()
{
    local label=$1
    local enabled=$2
    local expected_batches=$3
    local first_set_rows=$4
    local table="ready_subquery_union_${label}"

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};

        CREATE TABLE ${table}
        (
            tenant Int32,
            key_gap UInt16,
            item_key UInt128,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree ORDER BY (tenant, key_gap, item_key)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            max_row_mask_update_coalescing_commands = 4096, max_row_mask_update_coalescing_ast_bytes = 1048576,
            enable_row_mask_typed_set_union = ${enabled};

        INSERT INTO ${table}
        SELECT seed_tenant, seed_gap, toUInt128(seed_key), initializeAggregation('sumState', seed_value)
        FROM values('seed_tenant Int32, seed_gap UInt16, seed_key String, seed_value Int64',
            (-1, 0, '18446744073709551617', 10), (-1, 1, '2', 20), (-1, 1, '18446744073709551617', 70),
            (0, 0, '18446744073709551617', 30), (0, 1, '2', 40), (1, 0, '3', 50), (1, 1, '4', 60));

        ALTER TABLE ${table} UPDATE _row_exists = 0 WHERE tenant = 1 AND item_key = toUInt128(4)
        SETTINGS mutations_sync = 2;
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE (tenant, item_key) IN
        (
            SELECT arrayJoin(arrayZip(
                arrayResize([toInt32(-1), toInt32(0)], ${first_set_rows}, toInt32(2)),
                arrayConcat([toUInt128('18446744073709551617'), toUInt128(2)],
                    arrayMap(key -> toUInt128(key), range(${first_set_rows} - 2)))
            ))
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        DELETE FROM ${table} WHERE (tenant, item_key) IN
        (
            SELECT arrayJoin(arrayZip([toInt32(-1), toInt32(1)], [toUInt128('18446744073709551617'), toUInt128(3)]))
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
            sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${expected_batches},
            sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0
        ${log_filter};

        SELECT '${label}_mask', tenant, key_gap, item_key, _row_exists, finalizeAggregation(s)
        FROM ${table} ORDER BY tenant, key_gap, item_key
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_visible', tenant, key_gap, item_key, sumMerge(s)
        FROM ${table} GROUP BY tenant, key_gap, item_key ORDER BY tenant, key_gap, item_key
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table};
    "
}

run_case off 0 0 2
run_case on 1 1 2
# The first Set exceeds the union's row cap. Its extra keys use tenant 2, absent from the part.
run_case over_budget 1 0 4097
