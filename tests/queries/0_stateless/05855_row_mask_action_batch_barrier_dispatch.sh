#!/usr/bin/env bash
# Tags: no-replicated-database
# Keep the mixed commands in one `ALTER` to check their execution order.
# A `Replicated` database rejects this mixed command sequence before mutation dispatch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS mask_dispatch_barrier;

    CREATE TABLE mask_dispatch_barrier
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
        enable_row_mask_update_coalescing = 1, enable_row_mask_action_batching = 1,
        max_row_mask_update_coalescing_commands = 4096,
        max_row_mask_update_coalescing_ast_bytes = 1048576;

    INSERT INTO mask_dispatch_barrier
    SELECT toInt32(number % 3) - 1 AS dispatch_seed_tenant,
        toUInt128(number) AS dispatch_seed_item, sumState(toInt64(number + 1)), toUInt8(0)
    FROM numbers(12)
    GROUP BY dispatch_seed_tenant, dispatch_seed_item;
"

run_start=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")

$CLICKHOUSE_CLIENT --query "
    ALTER TABLE mask_dispatch_barrier
        UPDATE _row_exists = 0 WHERE tenant < 0 AND item < toUInt128(10),
        DROP COLUMN spare,
        UPDATE _row_exists = 0 WHERE tenant >= 0 AND item > toUInt128(3)
    SETTINGS mutations_sync = 2
"

# `mutations_sync` may return before the successful `MutatePart` entry is queued.
poll_deadline=$((SECONDS + 60))
logged=0
while (( SECONDS < poll_deadline ))
do
    poll_seconds=$((poll_deadline - SECONDS))
    logged=$(timeout --foreground -k 1 "$poll_seconds" $CLICKHOUSE_CLIENT --max_execution_time 10 --multiquery --query "
        SYSTEM FLUSH LOGS part_log;
        SELECT count() > 0
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
          AND database = currentDatabase() AND table = 'mask_dispatch_barrier'
          AND event_type = 'MutatePart' AND error = 0
          AND (table, table_uuid) IN
          (
              SELECT name, uuid FROM system.tables
              WHERE database = currentDatabase() AND name = 'mask_dispatch_barrier'
          )
    ")
    [[ "$logged" == 1 ]] && break
    sleep 0.1
done

if [[ "$logged" != 1 ]]
then
    echo 'Timed out waiting for row-mask barrier mutation log' >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --max_execution_time 10 --query "
    SELECT 'barrier', count() > 0 AND sum(ProfileEvents['RowMaskActionBatches']) = 0
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND toUnixTimestamp64Micro(event_time_microseconds) >= ${run_start}
      AND database = currentDatabase() AND table = 'mask_dispatch_barrier'
      AND event_type = 'MutatePart' AND error = 0
      AND (table, table_uuid) IN
      (
          SELECT name, uuid FROM system.tables
          WHERE database = currentDatabase() AND name = 'mask_dispatch_barrier'
      )
"

$CLICKHOUSE_CLIENT --query "DROP TABLE mask_dispatch_barrier"
