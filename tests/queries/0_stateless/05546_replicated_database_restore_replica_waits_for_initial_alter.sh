#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the `database_replicated_pause_before_initial_query_execution` failpoint fires globally,
#   it would pause (or be consumed by) a DDL query of another `Replicated` database.

# `SYSTEM RESTORE DATABASE REPLICA` waits for the initial DDL queries that the DDL worker has already admitted.
# An `ALTER` of a `ReplicatedMergeTree` table with `alter_sync = 1` waits until the table executes its
# `ALTER_METADATA` entry, which requires an active DDL worker of the database (`canExecuteReplicatedMetadataAlter`).
# The restore must not stop the worker before such a query finishes, otherwise both queries wait for each other.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
FAILPOINT="database_replicated_pause_before_initial_query_execution"
RESTORE_QUERY_ID="restore_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB} SYNC" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB}_broken_replicated_tables SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 'shard1', 'replica1')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "CREATE TABLE ${DB}.t (x UInt64) ENGINE = ReplicatedMergeTree ORDER BY x"

# Hold the initial `ALTER` after the DDL worker has admitted it, before it executes.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --alter_sync 1 --distributed_ddl_output_mode=none --query "ALTER TABLE ${DB}.t ADD COLUMN y UInt64" &
alter_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

$CLICKHOUSE_CLIENT --query_id "$RESTORE_QUERY_ID" --query "SYSTEM RESTORE DATABASE REPLICA ${DB}" >/dev/null 2>&1 &
restore_pid=$!

# The restore has to wait for the paused `ALTER`.
for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$RESTORE_QUERY_ID'")" = 1 ] && break
    sleep 0.1
done
sleep 2
$CLICKHOUSE_CLIENT --query "SELECT 'restore is waiting', count() FROM system.processes WHERE query_id = '$RESTORE_QUERY_ID'"

# The `ALTER` finishes, including the wait for its `ALTER_METADATA` entry, and then the restore proceeds.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"
wait $alter_pid && echo "alter succeeded"
# The replica is not lost (its digest is not empty), so the restore may refuse with `REPLICA_ALREADY_EXISTS`;
# either way the DDL worker is re-created.
wait $restore_pid ||:
echo "restore finished"

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC DATABASE REPLICA ${DB}"
$CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns WHERE database = '${DB}' AND table = 't' ORDER BY name"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${DB}_broken_replicated_tables'"
