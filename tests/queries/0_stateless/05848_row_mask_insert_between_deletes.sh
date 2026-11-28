#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

run_case()
{
    local label=$1
    local enabled=$2
    local partition_expr=$3
    local expected_partitions=$4
    local table="row_mask_insert_boundary_${label}"

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};

        CREATE TABLE ${table}
        (
            generation UInt8,
            id UInt8,
            item_key UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree
        PARTITION BY ${partition_expr}
        ORDER BY (generation, id, item_key)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
            enable_row_mask_typed_set_union = ${enabled};

        INSERT INTO ${table}
        SELECT generation, id, item_key, initializeAggregation('sumState', seed_value)
        FROM values('generation UInt8, id UInt8, item_key UInt64, seed_value Int64',
            (0, 0, 1, 10), (0, 1, 2, 20), (0, 2, 3, 30), (0, 3, 4, 40));
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE item_key IN
        (
            SELECT if(number = 0, toUInt64(1), toUInt64(9)) FROM numbers(2)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        INSERT INTO ${table}
        SELECT generation, id, item_key, initializeAggregation('sumState', seed_value)
        FROM values('generation UInt8, id UInt8, item_key UInt64, seed_value Int64',
            (1, 4, 1, 100), (1, 5, 2, 200), (1, 6, 3, 300));

        DELETE FROM ${table} WHERE item_key IN
        (
            SELECT if(number = 0, toUInt64(2), toUInt64(8)) FROM numbers(2)
        )
        SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;

        INSERT INTO ${table}
        SELECT generation, id, item_key, initializeAggregation('sumState', seed_value)
        FROM values('generation UInt8, id UInt8, item_key UInt64, seed_value Int64',
            (2, 7, 1, 700), (2, 8, 2, 800));
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
        $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"
        echo "Expected two queued row-mask mutations for ${table}" >&2
        return 1
    fi

    $CLICKHOUSE_CLIENT --query "
        SELECT '${label}_source_parts', count() = 3,
            arraySort(groupArray(rows)) = [2, 3, 4],
            uniqExact(partition_id) = ${expected_partitions}
        FROM system.parts
        WHERE database = currentDatabase() AND table = '${table}' AND active
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"

    local deadline=$((SECONDS + 60))
    local done=0
    while (( SECONDS < deadline ))
    do
        done=$(timeout --foreground -k 1 "$((deadline - SECONDS))" $CLICKHOUSE_CLIENT --max_execution_time 10 --query "
            SELECT count() = 2 AND countIf(is_done) = 2
            FROM system.mutations
            WHERE database = currentDatabase() AND table = '${table}'
        ")
        [[ "$done" == 1 ]] && break
        sleep 0.1
    done
    if [[ "$done" != 1 ]]
    then
        echo "Timed out waiting for mutations for ${table}" >&2
        return 1
    fi

    if [[ "$partition_expr" == 'tuple()' ]]
    then
        $CLICKHOUSE_CLIENT --query "
            SELECT '${label}_ids', arraySort(groupArray(id)) = [2, 3, 4, 6, 7, 8]
            FROM ${table}
            SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0
        "
    else
        local log_filter="
            FROM system.part_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600
              AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
              AND database = currentDatabase() AND table = '${table}'
              AND table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '${table}')
              AND event_type = 'MutatePart' AND error = 0 AND hasAny(mutation_ids, ${queued_ids})
        "
        deadline=$((SECONDS + 60))
        local logged=0
        while (( SECONDS < deadline ))
        do
            logged=$(timeout --foreground -k 1 "$((deadline - SECONDS))" $CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
                SYSTEM FLUSH LOGS part_log;
                SELECT count() >= 2 ${log_filter}
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
            SELECT '${label}_events', count() = 2,
                countIf(rows = 4 AND arraySort(mutation_ids) = ${queued_ids}) = 1,
                countIf(rows = 3 AND arraySort(mutation_ids) = [arrayElement(${queued_ids}, 2)]) = 1,
                sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${enabled},
                sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0
            ${log_filter};

            SELECT '${label}_mask', generation, id, item_key, _row_exists, finalizeAggregation(s)
            FROM ${table} ORDER BY id
            SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

            SELECT '${label}_active_parts', count() = 3,
                arraySort(groupArray(rows)) = [2, 3, 4],
                uniqExact(partition_id) = ${expected_partitions}
            FROM system.parts
            WHERE database = currentDatabase() AND table = '${table}' AND active;
        "
    fi

    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT '${label}_visible', count() = 6, sumMerge(s) = 1970
        FROM ${table}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table};
    "
}

run_case partitioned_off 0 generation 3
run_case partitioned_on 1 generation 3
run_case same_off 0 'tuple()' 1
run_case same_on 1 'tuple()' 1
