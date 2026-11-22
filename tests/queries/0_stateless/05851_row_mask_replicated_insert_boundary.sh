#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# Two explicit `ReplicatedAggregatingMergeTree` replicas use one Keeper path.
# The replicated-database and shared-merge-tree test modes rewrite this topology.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

active_r1=""
active_r2=""

cleanup_case()
{
    if [[ -n "$active_r1" ]]
    then
        $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${active_r1}" >/dev/null 2>&1 || true
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${active_r1} SYNC" >/dev/null 2>&1 || true
    fi
    if [[ -n "$active_r2" ]]
    then
        $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${active_r2}" >/dev/null 2>&1 || true
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${active_r2} SYNC" >/dev/null 2>&1 || true
    fi
}

trap cleanup_case EXIT

wait_for_true()
{
    local query=$1
    local description=$2
    local deadline=$((SECONDS + 60))
    local value=0
    while (( SECONDS < deadline ))
    do
        value=$($CLICKHOUSE_CLIENT --max_execution_time 10 --query "$query")
        [[ "$value" == 1 ]] && return 0
        sleep 0.1
    done
    echo "Timed out waiting for ${description}" >&2
    return 1
}

run_case()
{
    local label=$1
    local enabled=$2
    local r1="row_mask_replicated_boundary_${label}_r1"
    local r2="row_mask_replicated_boundary_${label}_r2"
    active_r1=$r1
    active_r2=$r2

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${r2} SYNC;
        DROP TABLE IF EXISTS ${r1} SYNC;

        CREATE TABLE ${r1}
        (
            generation UInt8,
            id UInt8,
            item_key UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/row_mask_replicated_boundary_${label}', 'r1')
        PARTITION BY generation
        ORDER BY (generation, id, item_key)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            max_replicated_mutations_in_queue = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            enable_row_mask_typed_set_union = ${enabled};

        CREATE TABLE ${r2}
        (
            generation UInt8,
            id UInt8,
            item_key UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/row_mask_replicated_boundary_${label}', 'r2')
        PARTITION BY generation
        ORDER BY (generation, id, item_key)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            max_replicated_mutations_in_queue = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            enable_row_mask_typed_set_union = ${enabled};

        INSERT INTO ${r1}
        SELECT generation, id, item_key, initializeAggregation('sumState', seed_value)
        FROM values('generation UInt8, id UInt8, item_key UInt64, seed_value Int64',
            (0, 0, 1, 10), (0, 1, 2, 20), (0, 2, 3, 30), (0, 3, 4, 40));

        SYSTEM SYNC REPLICA ${r2};
    "

    wait_for_true "
        SELECT count() = 2 AND countIf(total_replicas = 2 AND active_replicas = 2) = 2
        FROM system.replicas
        WHERE database = currentDatabase() AND table IN ('${r1}', '${r2}')
    " "both active replicas for ${label}"

    $CLICKHOUSE_CLIENT --query "
        SELECT '${label}_replicas', total_replicas = 2, active_replicas = 2
        FROM system.replicas
        WHERE database = currentDatabase() AND table IN ('${r1}', '${r2}')
        ORDER BY table
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${r1};
        SYSTEM STOP MERGES ${r2};

        DELETE FROM ${r1} WHERE item_key IN
        (
            SELECT if(number = 0, toUInt64(1), toUInt64(9)) FROM numbers(2)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0,
            allow_nondeterministic_mutations = 1;

        INSERT INTO ${r1}
        SELECT generation, id, item_key, initializeAggregation('sumState', seed_value)
        FROM values('generation UInt8, id UInt8, item_key UInt64, seed_value Int64',
            (1, 4, 1, 100), (1, 5, 2, 200), (1, 6, 3, 300));

        DELETE FROM ${r1} WHERE item_key IN
        (
            SELECT if(number = 0, toUInt64(2), toUInt64(8)) FROM numbers(2)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0,
            allow_nondeterministic_mutations = 1;

        INSERT INTO ${r1}
        SELECT generation, id, item_key, initializeAggregation('sumState', seed_value)
        FROM values('generation UInt8, id UInt8, item_key UInt64, seed_value Int64',
            (2, 7, 1, 700), (2, 8, 2, 800));

        SYSTEM SYNC REPLICA ${r1} PULL;
        SYSTEM SYNC REPLICA ${r2} PULL;
    "

    local mutation_ids=""
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        mutation_ids=$($CLICKHOUSE_CLIENT --query "
            SELECT arraySort(groupArray(mutation_id))
            FROM system.mutations
            WHERE database = currentDatabase() AND table = '${r1}'
            HAVING count() = 2 AND countIf(position(command, '_row_exists') > 0) = 2
        ")
        [[ -n "$mutation_ids" ]] && break
        sleep 0.1
    done
    if [[ -z "$mutation_ids" ]]
    then
        echo "Expected two row-mask mutations for ${r1}" >&2
        return 1
    fi

    wait_for_true "
        SELECT count() = 2 AND arraySort(groupArray(mutation_id)) = ${mutation_ids}
            AND countIf(position(command, '_row_exists') > 0) = 2
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '${r2}'
    " "both mutation IDs on ${r2}"

    $CLICKHOUSE_CLIENT --query "
        SELECT '${label}_source_parts', count() = 3,
            arraySort(groupArray(rows)) = [2, 3, 4], uniqExact(partition_id) = 3
        FROM system.parts
        WHERE database = currentDatabase() AND table = '${r1}' AND active
    "

    $CLICKHOUSE_CLIENT --query "ALTER TABLE ${r1} MODIFY SETTING max_replicated_mutations_in_queue = 8"
    $CLICKHOUSE_CLIENT --query "ALTER TABLE ${r2} MODIFY SETTING max_replicated_mutations_in_queue = 8"
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${r1}"
    wait_for_true "
        SELECT count() = 2 AND countIf(is_done) = 2
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '${r1}'
    " "mutations on ${r1}"

    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${r2}"
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA ${r2}"
    wait_for_true "
        SELECT count() = 2 AND countIf(is_done) = 2
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '${r2}'
    " "mutations on ${r2}"

    local log_filter="
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
          AND database = currentDatabase() AND table IN ('${r1}', '${r2}')
          AND table_uuid IN
          (
              SELECT uuid FROM system.tables
              WHERE database = currentDatabase() AND name IN ('${r1}', '${r2}')
          )
          AND event_type = 'MutatePart' AND error = 0 AND hasAny(mutation_ids, ${mutation_ids})
    "
    deadline=$((SECONDS + 60))
    local logged=0
    while (( SECONDS < deadline ))
    do
        logged=$($CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
            SYSTEM FLUSH LOGS part_log;
            SELECT countIf(rows = 4 AND arraySort(mutation_ids) = ${mutation_ids}) >= 1
                AND countIf(rows = 3 AND arraySort(mutation_ids) = [arrayElement(${mutation_ids}, 2)]) >= 1
            ${log_filter}
        ")
        [[ "$logged" == 1 ]] && break
        sleep 0.1
    done
    if [[ "$logged" != 1 ]]
    then
        echo "Timed out waiting for mutation part-log events for ${label}" >&2
        return 1
    fi

    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT '${label}_mutation_order',
            countIf(rows = 4 AND arraySort(mutation_ids) = ${mutation_ids}) >= 1,
            countIf(rows = 3 AND arraySort(mutation_ids) = [arrayElement(${mutation_ids}, 2)]) >= 1,
            (sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) > 0) = ${enabled}
        ${log_filter};

        SELECT '${label}_r1_mask', id, _row_exists, finalizeAggregation(s)
        FROM ${r1} ORDER BY id
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_r2_mask', id, _row_exists, finalizeAggregation(s)
        FROM ${r2} ORDER BY id
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_r1_visible', count() = 6,
            arraySort(groupArray(id)) = [2, 3, 4, 6, 7, 8], sumMerge(s) = 1970
        FROM ${r1}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        SELECT '${label}_r2_visible', count() = 6,
            arraySort(groupArray(id)) = [2, 3, 4, 6, 7, 8], sumMerge(s) = 1970
        FROM ${r2}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${r2} SYNC;
        DROP TABLE ${r1} SYNC;
    "
    active_r1=""
    active_r2=""
}

run_case off 0
run_case on 1
