#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel, no-fasttest: the failpoint is server-wide and can pause another table's mutation.
# no-replicated-database, no-shared-merge-tree: the pause must occur in this server's local mutation task.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

failpoint=mt_mutate_task_pause_after_ttl_mode_selection
active_table=

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 || true
    if [[ -n "$active_table" ]]
    then
        $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${active_table}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

run_case()
{
    local label=$1
    local enabled=$2
    local expected_batches=$3
    local table="row_mask_rhs_during_${label}"
    local rhs="row_mask_rhs_during_source_${label}"
    active_table=$table

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};
        DROP TABLE IF EXISTS ${rhs};

        CREATE TABLE ${rhs} (phase UInt8, item_key UInt64) ENGINE = Memory;
        INSERT INTO ${rhs} VALUES (1, 0), (2, 2);

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
            (0, 0, 10), (1, 1, 20), (2, 2, 30), (3, 3, 40),
            (4, 4, 50), (5, 5, 60), (6, 6, 70), (7, 7, 80));

        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE item_key IN
        (
            SELECT item_key FROM ${rhs} WHERE phase = 1
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        DELETE FROM ${table} WHERE item_key IN
        (
            SELECT item_key FROM ${rhs} WHERE phase = 2
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

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${failpoint}"
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"
    timeout --foreground -k 1 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

    # The main interpreter has not run yet. Its RHS Set may still come from
    # the task's precheck cache, so compare OFF and ON instead of assuming a
    # snapshot time for these new rows.
    $CLICKHOUSE_CLIENT --query "INSERT INTO ${rhs} VALUES (1, 6), (2, 7)"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}"

    local deadline=$((SECONDS + 60))
    local completed=0
    while (( SECONDS < deadline ))
    do
        completed=$($CLICKHOUSE_CLIENT --query "
            SELECT count() = 2 AND countIf(is_done) = 2
            FROM system.mutations
            WHERE database = currentDatabase() AND table = '${table}'
              AND has(${queued_ids}, mutation_id)
        ")
        [[ "$completed" == 1 ]] && break
        sleep 0.1
    done
    if [[ "$completed" != 1 ]]
    then
        echo "Timed out waiting for mutations of ${table}" >&2
        return 1
    fi

    local log_filter="
        FROM system.part_log
        WHERE database = currentDatabase() AND table = '${table}'
          AND table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '${table}')
          AND event_type = 'MutatePart' AND error = 0 AND hasAny(mutation_ids, ${queued_ids})
    "
    local logged=0
    while (( SECONDS < deadline ))
    do
        logged=$(timeout --foreground -k 1 "$((deadline - SECONDS))" $CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
            SYSTEM FLUSH LOGS part_log;
            SELECT count() > 0 ${log_filter}
        ")
        [[ "$logged" == 1 ]] && break
        sleep 0.1
    done
    if [[ "$logged" != 1 ]]
    then
        echo "Timed out waiting for part log of ${table}" >&2
        return 1
    fi

    $CLICKHOUSE_CLIENT --query "
        SELECT '${label}_dispatch', count() = 1 AND arraySort(any(mutation_ids)) = ${queued_ids},
            sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${expected_batches},
            sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0
        ${log_filter}
    "
    active_table=
}

run_case off 0 0
run_case on 1 1

$CLICKHOUSE_CLIENT --multiquery --query "
    SELECT 'rhs_rows',
        (SELECT count() FROM row_mask_rhs_during_source_off) = 4,
        (SELECT count() FROM row_mask_rhs_during_source_on) = 4;

    SELECT 'old_keys_deleted', count() = 8,
        countIf(_row_exists = 0 AND item_key IN (0, 2)) = 2,
        countIf(_row_exists = 0 AND item_key NOT IN (0, 2, 6, 7)) = 0
    FROM row_mask_rhs_during_off
    SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

    SELECT 'mask_parity',
        (SELECT count() FROM row_mask_rhs_during_on) = 8,
        (SELECT arraySort(groupArray((n, item_key, _row_exists, finalizeAggregation(s))))
         FROM row_mask_rhs_during_off) =
        (SELECT arraySort(groupArray((n, item_key, _row_exists, finalizeAggregation(s))))
         FROM row_mask_rhs_during_on)
    SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

    SELECT 'visible_parity',
        (SELECT arraySort(groupArray((n, item_key, finalizeAggregation(s))))
         FROM row_mask_rhs_during_off) =
        (SELECT arraySort(groupArray((n, item_key, finalizeAggregation(s))))
         FROM row_mask_rhs_during_on)
    SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

    DROP TABLE row_mask_rhs_during_off;
    DROP TABLE row_mask_rhs_during_on;
    DROP TABLE row_mask_rhs_during_source_off;
    DROP TABLE row_mask_rhs_during_source_on;
"
