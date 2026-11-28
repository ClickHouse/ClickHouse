#!/usr/bin/env bash
# Tags: shared-catalog

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# Keep unrelated parallel replica randomization out of this mutation test.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_parallel_replicas=0 --automatic_parallel_replicas_mode=0"

$CLICKHOUSE_CLIENT --multiquery <<'SQL'

DROP TABLE IF EXISTS test_05857_off;
DROP TABLE IF EXISTS test_05857_on;

CREATE TABLE test_05857_off
(
    k UInt64,
    s AggregateFunction(sum, Int64)
)
ENGINE = SharedAggregatingMergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    number_of_free_entries_in_pool_to_execute_mutation = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
    enable_row_mask_typed_set_union = 0;

CREATE TABLE test_05857_on
(
    k UInt64,
    s AggregateFunction(sum, Int64)
)
ENGINE = SharedAggregatingMergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    number_of_free_entries_in_pool_to_execute_mutation = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
    enable_row_mask_typed_set_union = 1;

INSERT INTO test_05857_off
SELECT k, sumState(v)
FROM values('k UInt64, v Int64', (1, 10), (2, 20), (3, 30), (4, 40))
GROUP BY k;
INSERT INTO test_05857_on SELECT * FROM test_05857_off;

-- Overlapping ready subquery `Set`s must agree with and without `enable_row_mask_typed_set_union`.
ALTER TABLE test_05857_off
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number * 2 + 1) FROM numbers(2)),
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number + 3) FROM numbers(2))
SETTINGS mutations_sync = 2;
ALTER TABLE test_05857_on
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number * 2 + 1) FROM numbers(2)),
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number + 3) FROM numbers(2))
SETTINGS mutations_sync = 2;

SELECT 'replicas', count() = 2 AND uniqExact(serverUUID()) = 2
FROM clusterAllReplicas('test_cluster_shared_catalog', system.one);
SQL

# `mutations_sync` may return before the `MutatePart` entries reach `part_log`.
# Wait for both tables before checking the dispatch counters.
poll_deadline=$((SECONDS + 60))
both_logged=0
while (( SECONDS < poll_deadline ))
do
    poll_seconds=$((poll_deadline - SECONDS))
    (( poll_seconds > 0 )) || break
    timeout --foreground -k 1 "$poll_seconds" $CLICKHOUSE_CLIENT --query \
        'SYSTEM FLUSH LOGS ON CLUSTER test_cluster_shared_catalog part_log' >/dev/null
    poll_seconds=$((poll_deadline - SECONDS))
    (( poll_seconds > 0 )) || break
    both_logged=$(timeout --foreground -k 1 "$poll_seconds" $CLICKHOUSE_CLIENT --max_execution_time 10 --query "
        SELECT countIf(table = 'test_05857_off') > 0 AND countIf(table = 'test_05857_on') > 0
        FROM clusterAllReplicas('test_cluster_shared_catalog', system.part_log)
        WHERE event_date >= yesterday()
            AND database = currentDatabase()
            AND (table, table_uuid) IN
            (
                SELECT name, uuid FROM system.tables
                WHERE database = currentDatabase() AND name IN ('test_05857_off', 'test_05857_on')
            )
            AND event_type = 'MutatePart' AND error = 0
    ")
    [[ "$both_logged" == 1 ]] && break
    sleep 0.1
done

if [[ "$both_logged" != 1 ]]
then
    echo 'Timed out waiting for Shared Catalog row-mask mutation logs' >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --multiquery <<'SQL'
SELECT 'dispatch',
    countIf(table = 'test_05857_off') > 0,
    sumIf(ProfileEvents['RowMaskReadySubquerySetUnionBatches'], table = 'test_05857_off') = 0,
    countIf(table = 'test_05857_on') > 0,
    sumIf(ProfileEvents['RowMaskReadySubquerySetUnionBatches'], table = 'test_05857_on') > 0
FROM clusterAllReplicas('test_cluster_shared_catalog', system.part_log)
WHERE event_date >= yesterday()
    AND database = currentDatabase()
    AND (table, table_uuid) IN
    (
        SELECT name, uuid FROM system.tables
        WHERE database = currentDatabase() AND name IN ('test_05857_off', 'test_05857_on')
    )
    AND event_type = 'MutatePart' AND error = 0;

SELECT 'before_off', k, sumMerge(s) FROM test_05857_off GROUP BY k ORDER BY k;
SELECT 'before_on', k, sumMerge(s) FROM test_05857_on GROUP BY k ORDER BY k;

-- Fresh states at deleted keys must remain visible after a merge.
INSERT INTO test_05857_off
SELECT k, sumState(v)
FROM values('k UInt64, v Int64', (1, 7), (3, 5))
GROUP BY k;
INSERT INTO test_05857_on
SELECT k, sumState(v)
FROM values('k UInt64, v Int64', (1, 7), (3, 5))
GROUP BY k;
OPTIMIZE TABLE test_05857_off FINAL SETTINGS alter_sync = 1;
OPTIMIZE TABLE test_05857_on FINAL SETTINGS alter_sync = 1;

SELECT 'after_off', k, sumMerge(s) FROM test_05857_off GROUP BY k ORDER BY k;
SELECT 'after_on', k, sumMerge(s) FROM test_05857_on GROUP BY k ORDER BY k;

DROP TABLE test_05857_off;
DROP TABLE test_05857_on;
SQL
