#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

active_table=""
active_rhs=""
active_filter=""

cleanup_case()
{
    if [[ -n "$active_table" ]]
    then
        $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${active_table}" >/dev/null 2>&1 || true
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${active_table} SYNC" >/dev/null 2>&1 || true
    fi
    if [[ -n "$active_rhs" ]]
    then
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${active_rhs} SYNC" >/dev/null 2>&1 || true
    fi
    if [[ -n "$active_filter" ]]
    then
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${active_filter} SYNC" >/dev/null 2>&1 || true
    fi
}

trap cleanup_case EXIT

run_case()
{
    local label=$1
    local enabled=$2
    local table="row_mask_nested_rhs_${label}"
    local rhs="row_mask_nested_source_${label}"
    local filter="row_mask_nested_filter_${label}"
    active_table=$table
    active_rhs=$rhs
    active_filter=$filter

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table} SYNC;
        DROP TABLE IF EXISTS ${rhs} SYNC;
        DROP TABLE IF EXISTS ${filter} SYNC;

        CREATE TABLE ${rhs} (phase UInt8, item_key UInt64) ENGINE = Memory;
        INSERT INTO ${rhs} VALUES (1, 0), (1, 2), (1, 2), (2, 2), (2, 4), (2, 4);

        CREATE TABLE ${filter} (item_key UInt64, enabled UInt8) ENGINE = Memory;
        INSERT INTO ${filter} VALUES (0, 1), (2, 1), (4, 1), (5, 0);

        CREATE TABLE ${table}
        (
            n UInt8,
            item_key UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree ORDER BY (item_key, n)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            max_row_mask_update_coalescing_commands = 4096, max_row_mask_update_coalescing_ast_bytes = 1048576,
            enable_row_mask_typed_set_union = ${enabled};

        INSERT INTO ${table}
        SELECT n, item_key, initializeAggregation('sumState', value)
        FROM values('n UInt8, item_key UInt64, value Int64',
            (0, 0, 10), (1, 1, 20), (2, 2, 30),
            (3, 3, 40), (4, 4, 50), (5, 5, 60));

        SELECT '${label}_rhs',
            arraySort(groupUniqArrayIf(item_key, phase = 1)) = [0, 2],
            arraySort(groupUniqArrayIf(item_key, phase = 2)) = [2, 4]
        FROM ${rhs};
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE item_key IN
        (
            WITH selected_keys AS
            (
                SELECT r.item_key AS item_key
                FROM ${rhs} AS r
                INNER JOIN ${filter} AS f ON r.item_key = f.item_key
                WHERE r.phase = 1 AND f.enabled = 1
            )
            SELECT item_key FROM (SELECT item_key FROM selected_keys)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        DELETE FROM ${table} WHERE item_key IN
        (
            SELECT item_key FROM
            (
                SELECT DISTINCT r.item_key AS item_key
                FROM ${rhs} AS r
                INNER JOIN ${filter} AS f ON r.item_key = f.item_key
                WHERE r.phase = 2 AND f.enabled = 1
            )
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
    if [[ -z "$queued_ids" ]]
    then
        echo "Expected two queued row-mask mutations for ${table}" >&2
        return 1
    fi
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"

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
        logged=$(timeout --foreground -k 1 12 $CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
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
            sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${enabled},
            sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0
        ${log_filter};

        SELECT '${label}_mask', n, item_key, _row_exists, finalizeAggregation(s)
        FROM ${table} ORDER BY n
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_visible', count() = 3,
            arraySort(groupArray(n)) = [1, 3, 5], sumMerge(s) = 120
        FROM ${table}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table} SYNC;
        DROP TABLE ${rhs} SYNC;
        DROP TABLE ${filter} SYNC;
    "
    active_table=""
    active_rhs=""
    active_filter=""
}

run_case off 0
run_case on 1
