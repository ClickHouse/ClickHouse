#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the `database_replicated_pause_before_initial_query_execution` failpoint fires globally,
#   it would pause (or be consumed by) a DDL query of another `Replicated` database.

# An initial DDL query of a `Replicated` database executes and commits on the submitting thread.
# `SYSTEM RESTORE DATABASE REPLICA` must wait for such a query that was already admitted by the old
# DDL worker before the new worker recovers the replica. Otherwise the query could commit its metadata
# while the recovery compares the Keeper snapshot with the local tables, and the recovery would move
# the just created table to the `<db>_broken_replicated_tables` database.

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

# Hold the initial `CREATE TABLE` after the DDL worker has admitted it, before it executes and commits.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "CREATE TABLE ${DB}.t (x UInt64) ENGINE = MergeTree ORDER BY x" &
create_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

$CLICKHOUSE_CLIENT --query_id "$RESTORE_QUERY_ID" --query "SYSTEM RESTORE DATABASE REPLICA ${DB}" >/dev/null 2>&1 &
restore_pid=$!

# The restore stops the old DDL worker and then has to wait for the paused `CREATE TABLE`.
for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$RESTORE_QUERY_ID'")" = 1 ] && break
    sleep 0.1
done
sleep 2
$CLICKHOUSE_CLIENT --query "SELECT 'restore is waiting', count() FROM system.processes WHERE query_id = '$RESTORE_QUERY_ID'"

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"
wait $create_pid && echo "create succeeded"
# The restore proceeds only after the query has committed. The replica is not lost then (its digest is not empty),
# so the restore may refuse with `REPLICA_ALREADY_EXISTS`; either way the DDL worker is re-created.
wait $restore_pid ||:
echo "restore finished"

# The table created by the successful query is still there after the recovery of the replica.
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC DATABASE REPLICA ${DB}"
$CLICKHOUSE_CLIENT --query "EXISTS TABLE ${DB}.t"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${DB}_broken_replicated_tables'"
