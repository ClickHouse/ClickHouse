#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: creates and drops global SQL-managed clusters

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# Lookups prefer the cluster of a `Replicated` database over a SQL-managed cluster with the same name.
# Creating, attaching or renaming such a database must therefore not take over the name of an existing
# SQL cluster: it would replace its topology without the `SQL CLUSTER ADMIN` privilege.

user="u_sql_cluster_db_owner_${CLICKHOUSE_DATABASE}"
cluster="c_sql_cluster_db_owner_${CLICKHOUSE_DATABASE}"
db="${cluster}_db"
zk_path="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${cluster}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "
        DROP DATABASE IF EXISTS ${cluster} SYNC;
        DROP DATABASE IF EXISTS ${db} SYNC;
        DROP CLUSTER IF EXISTS ${cluster};
        DROP CLUSTER IF EXISTS ${db};
        DROP USER IF EXISTS ${user};
    "
}

trap cleanup EXIT
cleanup

check()
{
    local output
    if output="$("$@" 2>&1)"
    then
        echo "OK"
    elif echo "${output}" | grep -q "CLUSTER_ALREADY_EXISTS"
    then
        echo "CLUSTER_ALREADY_EXISTS"
    else
        echo "${output}"
    fi
}

replica="REPLICA (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP})"
$CLICKHOUSE_CLIENT -q "CREATE CLUSTER ${cluster} (SHARD (${replica}), SHARD (${replica}))"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM cluster('${cluster}', system.one)"

echo "-- a user with CREATE DATABASE and TABLE ENGINE ON * cannot take over the name of a SQL cluster"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user}; GRANT CREATE DATABASE ON *.* TO ${user}; GRANT TABLE ENGINE ON * TO ${user}"
check $CLICKHOUSE_CLIENT --user "${user}" -q "CREATE DATABASE ${cluster} ENGINE = Replicated('${zk_path}/user', 's1', 'r1')"
echo "-- neither can the administrator"
check $CLICKHOUSE_CLIENT -q "CREATE DATABASE ${cluster} ENGINE = Replicated('${zk_path}/admin', 's1', 'r1')"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${cluster}'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM cluster('${cluster}', system.one)"

echo "-- nor rename a Replicated database to that name"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db} ENGINE = Replicated('${zk_path}/rename', 's1', 'r1')"
check $CLICKHOUSE_CLIENT -q "RENAME DATABASE ${db} TO ${cluster}"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM cluster('${cluster}', system.one)"

echo "-- a database of another engine does not own a cluster name"
check $CLICKHOUSE_CLIENT -q "CREATE DATABASE ${cluster} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM cluster('${cluster}', system.one)"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${cluster} SYNC"

echo "-- and a SQL cluster cannot take the name of a Replicated database"
check $CLICKHOUSE_CLIENT -q "CREATE CLUSTER ${db} (SHARD (${replica}), SHARD (${replica}))"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM cluster('${db}', system.one)"
