#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: creates a global SQL-managed cluster and pauses a process-wide failpoint.
# no-fasttest: inter-server `secret` needs SSL, and the fast test build has no failpoints.

# The first async insert copies the cluster, then builds the queue pool from that copy.
# Changing only the secret while it is paused, and changing it back afterwards, must not
# leave the queue signing with the intermediate secret.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_sql_cluster_05837"
data="data_05837"
dist="dist_05837"
failpoint="distributed_async_insert_queue_pause_before_pool_cluster_lookup"
insert_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.insert.log"
insert_pid=""

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "SYSTEM START DISTRIBUTED SENDS ${dist}" >/dev/null 2>&1 || :
    if [[ -n "${insert_pid}" ]]; then
        wait "${insert_pid}" >/dev/null 2>&1 || :
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}" >/dev/null 2>&1 || :
    rm -f "${insert_log}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}"
$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${cluster} (
    secret = 'secret_05837_A',
    SHARD (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP})
)
"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${data} (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${dist} AS ${data} ENGINE = Distributed(${cluster}, currentDatabase(), ${data})"

# Hold the files until the secret is back to A, so nothing is sent while the server expects B.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP DISTRIBUTED SENDS ${dist}"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 1" >"${insert_log}" 2>&1 &
insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

$CLICKHOUSE_CLIENT --query "
ALTER CLUSTER ${cluster} (
    secret = 'secret_05837_B',
    SHARD (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP})
)
"

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${failpoint}"

if ! wait "${insert_pid}"; then
    echo "insert failed"
    cat "${insert_log}"
    exit 1
fi
insert_pid=""

$CLICKHOUSE_CLIENT --query "
ALTER CLUSTER ${cluster} (
    secret = 'secret_05837_A',
    SHARD (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP})
)
"

$CLICKHOUSE_CLIENT --query "SYSTEM START DISTRIBUTED SENDS ${dist}"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 2"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${data}"
