#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# no-replicated-database: DETACH/ATTACH and the timed-out queries would go through the database's DDL queue
# no-shared-merge-tree: checks the loading of outdated parts from the table's own disk

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The two merged parts are outdated: after ATTACH they are loaded in the background, after a 2 minute sleep.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_outdated_wait;
    CREATE TABLE t_outdated_wait (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS old_parts_lifetime = 3600, sleep_before_loading_outdated_parts_ms = 120000;
    INSERT INTO t_outdated_wait VALUES (1);
    INSERT INTO t_outdated_wait VALUES (2);
    OPTIMIZE TABLE t_outdated_wait FINAL SETTINGS optimize_throw_if_noop = 1;
    DETACH TABLE t_outdated_wait;
    ATTACH TABLE t_outdated_wait;
    SELECT countIf(active), countIf(NOT active) FROM system.parts WHERE database = currentDatabase() AND table = 't_outdated_wait';
"

$CLICKHOUSE_CLIENT --max_execution_time 1 -q "TRUNCATE TABLE t_outdated_wait" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'
# There is no partial result, so the wait throws with timeout_overflow_mode = 'break' too.
$CLICKHOUSE_CLIENT --max_execution_time 1 -q "ALTER TABLE t_outdated_wait DROP PARTITION tuple() SETTINGS timeout_overflow_mode = 'break'" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'
$CLICKHOUSE_CLIENT --max_execution_time 1 -q "SYSTEM WAIT LOADING PARTS t_outdated_wait" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'

# KILL QUERY stops the same wait. This TRUNCATE has no time limit, so only the kill can end it before the sleep does.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "TRUNCATE TABLE t_outdated_wait" 2>&1 | grep -o -m1 'QUERY_WAS_CANCELLED' &
pid=$!
wait_for_query_to_start "$query_id" 60 "$pid"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"
timeout 30 tail --pid="$pid" -f /dev/null || echo "TRUNCATE ignored KILL QUERY"
wait

# The cancelled queries changed nothing.
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_outdated_wait"

# Dropping the table stops the sleep instead of waiting for it.
$CLICKHOUSE_CLIENT -q "DROP TABLE t_outdated_wait SYNC"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query_duration_ms < 60000 FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query_kind = 'Drop' AND query LIKE '%DROP TABLE t_outdated_wait SYNC%';
"
