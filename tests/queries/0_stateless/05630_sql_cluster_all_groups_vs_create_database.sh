#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest
# no-parallel: pauses `CREATE CLUSTER` with a server-global failpoint.
# no-fasttest: enables a server-global failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE CLUSTER all_groups.db` claims the name of the `Replicated` database `db`. A concurrent
# `CREATE DATABASE db ENGINE = Replicated` must not slip in between the ownership check and the
# installation of the SQL cluster, or both would succeed and the database would replace its topology.

db="d05630_${CLICKHOUSE_DATABASE}"
cluster="all_groups.${db}"
zk_path="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db}"
create_cluster_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.cluster.log"
create_database_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.database.log"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_create_database"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT sql_cluster_create_pause_after_namespace_check" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db} SYNC" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT -q "DROP CLUSTER IF EXISTS \`${cluster}\`" >/dev/null 2>&1 || :
    rm -f "$create_cluster_log" "$create_database_log"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT sql_cluster_create_pause_after_namespace_check"

$CLICKHOUSE_CLIENT -q "CREATE CLUSTER \`${cluster}\` (SHARD (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}))" >"$create_cluster_log" 2>&1 &
create_cluster_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT sql_cluster_create_pause_after_namespace_check PAUSE"

$CLICKHOUSE_CLIENT --query_id "$query_id" -q "CREATE DATABASE ${db} ENGINE = Replicated('${zk_path}', 's1', 'r1')" >"$create_database_log" 2>&1 &
create_database_pid=$!

# Let `CREATE DATABASE` reach the database guard, which `CREATE CLUSTER` holds while it is paused.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${query_id}'")" = "1" ] && break
    [ -d "/proc/${create_database_pid}" ] || break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT sql_cluster_create_pause_after_namespace_check"
wait "$create_cluster_pid"
echo "create cluster: $?"
cat "$create_cluster_log"

wait "$create_database_pid"
if grep -q "CLUSTER_ALREADY_EXISTS" "$create_database_log"
then
    echo "create database: CLUSTER_ALREADY_EXISTS"
else
    echo "create database: unexpected"
    cat "$create_database_log"
fi

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${db}'"
$CLICKHOUSE_CLIENT -q "SELECT source FROM system.clusters WHERE cluster = '${cluster}'"
