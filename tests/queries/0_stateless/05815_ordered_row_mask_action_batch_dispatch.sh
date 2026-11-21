#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS mask_dispatch_off;
    DROP TABLE IF EXISTS mask_dispatch_on;
    DROP TABLE IF EXISTS mask_dispatch_barrier;

    CREATE TABLE mask_dispatch_off
    (
        tenant Int32,
        item UInt128,
        s AggregateFunction(sum, Int64),
        spare SimpleAggregateFunction(anyLast, UInt8)
    )
    ENGINE = AggregatingMergeTree ORDER BY (tenant, item)
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
        enable_block_number_column = 0, enable_block_offset_column = 0,
        enable_row_mask_update_coalescing = 1, enable_row_mask_action_batching = 0,
        max_row_mask_update_coalescing_commands = 4096,
        max_row_mask_update_coalescing_ast_bytes = 1048576;

    CREATE TABLE mask_dispatch_on AS mask_dispatch_off
    SETTINGS enable_row_mask_action_batching = 1;
    CREATE TABLE mask_dispatch_barrier AS mask_dispatch_off
    SETTINGS enable_row_mask_action_batching = 1;

    INSERT INTO mask_dispatch_off
    SELECT toInt32(number % 3) - 1 AS dispatch_seed_tenant,
        toUInt128(number) AS dispatch_seed_item, sumState(toInt64(number + 1)), toUInt8(0)
    FROM numbers(12)
    GROUP BY dispatch_seed_tenant, dispatch_seed_item;
    INSERT INTO mask_dispatch_on SELECT * FROM mask_dispatch_off;
    INSERT INTO mask_dispatch_barrier SELECT * FROM mask_dispatch_off;
"

run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")

$CLICKHOUSE_CLIENT --multiquery --query "
    ALTER TABLE mask_dispatch_off
        UPDATE _row_exists = 0 WHERE tenant < 0 AND item < toUInt128(10),
        UPDATE _row_exists = 0 WHERE tenant >= 0 AND item > toUInt128(3)
    SETTINGS mutations_sync = 2;
    ALTER TABLE mask_dispatch_on
        UPDATE _row_exists = 0 WHERE tenant < 0 AND item < toUInt128(10),
        UPDATE _row_exists = 0 WHERE tenant >= 0 AND item > toUInt128(3)
    SETTINGS mutations_sync = 2;
    ALTER TABLE mask_dispatch_barrier
        UPDATE _row_exists = 0 WHERE tenant < 0 AND item < toUInt128(10),
        DROP COLUMN spare,
        UPDATE _row_exists = 0 WHERE tenant >= 0 AND item > toUInt128(3)
    SETTINGS mutations_sync = 2;
"

# `mutations_sync` may return before the successful `MutatePart` entry is queued.
# Wait for every table's row before checking both positive and zero event counts.
poll_deadline=$((SECONDS + 60))
all_logged=0
while (( SECONDS < poll_deadline ))
do
    poll_seconds=$((poll_deadline - SECONDS))
    all_logged=$(timeout --foreground -k 1 "$poll_seconds" $CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
        SYSTEM FLUSH LOGS part_log;
        SELECT uniqExact(tuple(table, table_uuid)) = 3
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
          AND database = currentDatabase() AND event_type = 'MutatePart' AND error = 0
          AND (table, table_uuid) IN
          (
              SELECT name, uuid FROM system.tables
              WHERE database = currentDatabase()
                AND name IN ('mask_dispatch_off', 'mask_dispatch_on', 'mask_dispatch_barrier')
          )
    ")
    [[ "$all_logged" == 1 ]] && break
    sleep 0.1
done

if [[ "$all_logged" != 1 ]]
then
    echo 'Timed out waiting for row-mask dispatch mutation logs' >&2
    exit 1
fi

assert_dispatch()
{
    local dispatch_table=$1
    local dispatch_label=$2
    local dispatch_comparison=$3
    $CLICKHOUSE_CLIENT --max_execution_time 10 --query "
        SELECT '${dispatch_label}', count() > 0 AND sum(ProfileEvents['RowMaskActionBatches']) ${dispatch_comparison}
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
          AND database = currentDatabase() AND table = '${dispatch_table}'
          AND event_type = 'MutatePart' AND error = 0
          AND (table, table_uuid) IN
          (
              SELECT name, uuid FROM system.tables
              WHERE database = currentDatabase() AND name = '${dispatch_table}'
          )
    "
}

assert_dispatch mask_dispatch_off off '= 0'
assert_dispatch mask_dispatch_on on '> 0'
assert_dispatch mask_dispatch_barrier barrier '= 0'

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE mask_dispatch_off;
    DROP TABLE mask_dispatch_on;
    DROP TABLE mask_dispatch_barrier;
"
