#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: creates a global SQL-managed cluster and pauses a process-wide failpoint.
# no-fasttest: failpoints are not built into the fast test binary.

# The first async insert creates the `Distributed` queue. Pausing after the version snapshot and
# dropping the cluster makes `parseAddresses` throw. The map must not keep a null queue: reading
# `system.distribution_queue` and `TRUNCATE` have to succeed, and a later insert must recover.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_sql_cluster_05830"
data="data_05830"
dist="dist_05830"
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
    rm -f "${insert_log}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}"
$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${cluster} (
    SHARD (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP})
)
"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${data} (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${dist} AS ${data} ENGINE = Distributed(${cluster}, currentDatabase(), ${data})"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 1" >"${insert_log}" 2>&1 &
insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"
$CLICKHOUSE_CLIENT --query "DROP CLUSTER ${cluster}"
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${failpoint}"

if wait "${insert_pid}"; then
    echo "insert succeeded"
    cat "${insert_log}"
    exit 1
fi
insert_pid=""

if ! grep -q 'CLUSTER_DOESNT_EXIST' "${insert_log}"; then
    echo "insert failed for another reason"
    cat "${insert_log}"
    exit 1
fi

$CLICKHOUSE_CLIENT --query "
SELECT count()
FROM system.distribution_queue
WHERE database = currentDatabase() AND table = '${dist}'
"
$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE ${dist}"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${cluster} (
    SHARD (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP})
)
"

$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 1"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${data}"
$CLICKHOUSE_CLIENT --query "
SELECT count()
FROM system.distribution_queue
WHERE database = currentDatabase() AND table = '${dist}'
"
