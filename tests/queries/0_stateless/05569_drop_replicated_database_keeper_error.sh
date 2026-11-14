#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the failpoints fire on the shutdown of the DDL worker of every `Replicated` database on the server.

# `DatabaseReplicatedDDLWorker::shutdown` removes the replica's ephemeral `active` node. A hardware Keeper
# error there must not fail `DROP DATABASE`, while `DETACH DATABASE` and non-hardware errors still report it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT="database_replicated_fail_active_node_removal_on_shutdown"
NONRETRYABLE_FAILPOINT="database_replicated_fail_active_node_removal_nonretryable"
INJECTED="Injected Keeper error while removing the active node"
NONRETRYABLE_INJECTED="Injected non-retryable Keeper error while removing the active node"
DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $NONRETRYABLE_FAILPOINT" >/dev/null 2>&1 ||:
    for name in drop detach nonretryable restore; do
        $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB}_${name} SYNC" >/dev/null 2>&1 ||:
    done
}
trap cleanup EXIT

function create_database()
{
    $CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB}_$1 ENGINE = Replicated('${ZK_PATH}/$1', 'shard1', 'replica1')"
    # Executed through the DDL worker, so afterwards the worker is initialized and holds the `active` node.
    $CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "CREATE TABLE ${DB}_$1.t (a UInt64) ENGINE = MergeTree ORDER BY a"
    active_node "$1"
}

function active_node()
{
    $CLICKHOUSE_CLIENT --query "SELECT 'active node', count() FROM system.zookeeper WHERE path = '${ZK_PATH}/$1/replicas/shard1|replica1' AND name = 'active'"
}

function database_exists()
{
    $CLICKHOUSE_CLIENT --query "SELECT 'database exists', count() FROM system.databases WHERE name = '${DB}_$1'"
}

echo "DROP with a hardware Keeper error"
create_database drop
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --send_logs_level=error --query "DROP DATABASE ${DB}_drop SYNC" 2>&1 \
    | grep -F "$INJECTED" | grep -c -F "before dropping it"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
database_exists drop
$CLICKHOUSE_CLIENT --query "SELECT 'keeper path exists', count() FROM system.zookeeper WHERE path = '${ZK_PATH}' AND name = 'drop'"

echo "DETACH with a hardware Keeper error"
create_database detach
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "DETACH DATABASE ${DB}_detach" 2>&1 | grep -q -F "$INJECTED" && echo "error reported"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
database_exists detach
# The failed eager removal is retried when the node holder is released.
active_node detach

echo "DROP with a non-hardware Keeper error"
create_database nonretryable
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $NONRETRYABLE_FAILPOINT"
$CLICKHOUSE_CLIENT --query "DROP DATABASE ${DB}_nonretryable SYNC" 2>&1 | grep -q -F "$NONRETRYABLE_INJECTED" && echo "error reported"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $NONRETRYABLE_FAILPOINT"
database_exists nonretryable

echo "RESTORE DATABASE REPLICA after a failed shutdown"
create_database restore
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "SYSTEM RESTORE DATABASE REPLICA ${DB}_restore" 2>&1 | grep -q -F "$INJECTED" && echo "error reported"
# With the failpoint still enabled, the second restore stops the same DDL worker again and must not repeat the removal.
# The replica is not lost, so the restore itself may refuse with `REPLICA_ALREADY_EXISTS`.
$CLICKHOUSE_CLIENT --query "SYSTEM RESTORE DATABASE REPLICA ${DB}_restore" 2>&1 | grep -c -F "$INJECTED"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "CREATE TABLE ${DB}_restore.t2 (a UInt64) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT --query "EXISTS TABLE ${DB}_restore.t2"
