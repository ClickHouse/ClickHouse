#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: the failpoint pauses any `SYSTEM RESTART REPLICA` on the server.
# Tag no-replicated-database: the failpoint is enabled on one server only.
# Tag no-shared-merge-tree: `SharedMergeTree` is not supported in a database without UUIDs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restart_replica_pause_after_detach" ||:' EXIT

# A `Memory` database drops tables synchronously, without the drop task of an `Atomic` one.
MEMORY_DATABASE="${CLICKHOUSE_DATABASE}_memory"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE DATABASE $MEMORY_DATABASE ENGINE = Memory;
    CREATE MATERIALIZED VIEW $MEMORY_DATABASE.mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv_memory', 'r1') ORDER BY x
        AS SELECT x FROM $CLICKHOUSE_DATABASE.src;
"
inner=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '$MEMORY_DATABASE' AND name LIKE '.inner%'")
[ -n "$inner" ] || { echo "No inner table of mv"; exit 1; }

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT restart_replica_pause_after_detach"
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA $MEMORY_DATABASE.\`$inner\`" &
restart_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT restart_replica_pause_after_detach PAUSE"

$CLICKHOUSE_CLIENT -q "DROP TABLE $MEMORY_DATABASE.mv" &
drop_pid=$!
# A DROP that does not wait for the restart finishes here, and the restart attaches the inner table back after it.
for _ in {1..30}; do kill -0 $drop_pid 2>/dev/null || break; sleep 0.1; done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restart_replica_pause_after_detach"
wait $restart_pid $drop_pid
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '$MEMORY_DATABASE' AND name LIKE '.inner%'"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $MEMORY_DATABASE"
