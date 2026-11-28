#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

run_case()
{
    local label=$1
    local enabled=$2
    local expected_ready_batches=$3
    local expected_action_batches=$4
    local fifth_group=$5
    local expected_sets=$6
    local expected_visible=$((1 - fifth_group))
    local expected_sum=$((90 * expected_visible))
    local table="ready_four_groups_${label}"

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS ${table};

        CREATE TABLE ${table}
        (
            n UInt8,
            a UInt64,
            b UInt64,
            c UInt64,
            d UInt64,
            e UInt64,
            s AggregateFunction(sum, Int64)
        )
        ENGINE = AggregatingMergeTree ORDER BY (a, b, c, d, e, n)
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
            number_of_free_entries_in_pool_to_execute_mutation = 0,
            enable_block_number_column = 0, enable_block_offset_column = 0,
            enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 1,
            enable_row_mask_typed_set_union = ${enabled};

        INSERT INTO ${table}
        SELECT toUInt8(number), number, number + 10, number + 20, number + 30,
            number + 40, initializeAggregation('sumState', toInt64((number + 1) * 10))
        FROM numbers(9);

        ALTER TABLE ${table} UPDATE _row_exists = 0 WHERE n = 7 SETTINGS mutations_sync = 2;
    "

    local run_start
    run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES ${table};

        DELETE FROM ${table} WHERE a IN
            (SELECT if(number = 0, toUInt64(0), toUInt64(2)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE b IN
            (SELECT if(number = 0, toUInt64(11), toUInt64(13)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE c IN
            (SELECT if(number = 0, toUInt64(22), toUInt64(24)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE d IN
            (SELECT if(number = 0, toUInt64(33), toUInt64(35)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE a IN
            (SELECT if(number = 0, toUInt64(2), toUInt64(4)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE b IN
            (SELECT if(number = 0, toUInt64(13), toUInt64(15)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE c IN
            (SELECT if(number = 0, toUInt64(24), toUInt64(26)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
        DELETE FROM ${table} WHERE d IN
            (SELECT if(number = 0, toUInt64(35), toUInt64(37)) FROM numbers(2))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
    "
    if [[ "$fifth_group" == 1 ]]
    then
        $CLICKHOUSE_CLIENT --query "
            DELETE FROM ${table} WHERE e IN (SELECT toUInt64(48) FROM numbers(1))
            SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0
        "
    fi

    local queued_ids
    queued_ids=$($CLICKHOUSE_CLIENT --query "
        SELECT arraySort(groupArray(mutation_id))
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done
        HAVING count() = ${expected_sets} AND countIf(position(command, '_row_exists') > 0) = ${expected_sets}
    ")
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ${table}"
    if [[ -z "$queued_ids" ]]
    then
        echo "Expected ${expected_sets} queued row-mask mutations for ${table}" >&2
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
        SELECT '${label}_dispatch', count() = 1 AND arraySort(arrayFlatten(groupArray(mutation_ids))) = ${queued_ids},
            sum(ProfileEvents['RowMaskReadySubquerySetUnionBatches']) = ${expected_ready_batches},
            sum(ProfileEvents['RowMaskActionBatches']) = ${expected_action_batches},
            sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = 0,
            sum(ProfileEvents['SetsBuiltFromSubquery']) = ${expected_sets}
        ${log_filter};

        SELECT '${label}_mask', n, a, b, c, d, e, _row_exists, finalizeAggregation(s)
        FROM ${table} ORDER BY n
        SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0;

        SELECT '${label}_visible', count() = ${expected_visible}, sumMerge(s) = ${expected_sum}
        FROM ${table}
        SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0;

        DROP TABLE ${table};
    "
}

run_case off 0 0 1 0 8
run_case on 1 1 0 0 8
run_case five 1 0 1 1 9
