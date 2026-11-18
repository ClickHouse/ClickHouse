#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: creates a global SQL-managed cluster, two users, and pauses a process-wide failpoint.
# no-fasttest: failpoints are not built into the fast test binary.

# The first async insert creates the `Distributed` queue. It samples `clusters_version` and then
# reads the topology. An `ALTER CLUSTER` in between must not pin the new version to the old host:
# this insert, and the next one, both have to reach the host from the altered cluster.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_sql_cluster_05633"
data="data_05633"
dist="dist_05633"
user_a="host_a_${CLICKHOUSE_DATABASE}"
user_b="host_b_${CLICKHOUSE_DATABASE}"
failpoint="distributed_async_insert_queue_pause_after_version_snapshot"
insert_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.insert.log"
insert_pid=""

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 || :
    if [[ -n "${insert_pid}" ]]; then
        wait "${insert_pid}" >/dev/null 2>&1 || :
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_a}, ${user_b}" >/dev/null 2>&1 || :
    rm -f "${insert_log}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}"
$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_a}, ${user_b}"

$CLICKHOUSE_CLIENT --query "CREATE USER ${user_a} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user_b} IDENTIFIED WITH no_password"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${cluster} (
    SHARD (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = '${user_a}')
)
"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${data} (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${dist} AS ${data} ENGINE = Distributed(${cluster}, currentDatabase(), ${data})"
$CLICKHOUSE_CLIENT --query "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.${data} TO ${user_a}, ${user_b}"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 --log_comment '05633_overlapped' \
    --query "INSERT INTO ${dist} (x) SELECT 1" >"${insert_log}" 2>&1 &
insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

$CLICKHOUSE_CLIENT --query "
ALTER CLUSTER ${cluster} (
    SHARD (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP}, user = '${user_b}')
)
"

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${failpoint}"

if ! wait "${insert_pid}"; then
    echo "insert failed"
    cat "${insert_log}"
    exit 1
fi
insert_pid=""

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
SELECT if(user = '${user_b}', 'new_host', user)
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND has(databases, currentDatabase())
  AND log_comment = '05633_overlapped'
  AND user IN ('${user_a}', '${user_b}')
ORDER BY event_time_microseconds
LIMIT 1
"

$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 --log_comment '05633_next' \
    --query "INSERT INTO ${dist} (x) SELECT 1"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
SELECT if(user = '${user_b}', 'new_host', user)
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND has(databases, currentDatabase())
  AND log_comment = '05633_next'
  AND user IN ('${user_a}', '${user_b}')
ORDER BY event_time_microseconds
LIMIT 1
"
