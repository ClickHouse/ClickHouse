#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: creates a global SQL-managed cluster and pauses a process-wide failpoint.
# no-fasttest: inter-server `secret` needs SSL, and the fast test build has no failpoints.

# `DistributedAsyncInsertBatch::sendBatch` borrows one connection for every file of the batch.
# `IConnectionPool::Entry` does not keep the pool alive. The failpoint pauses after the first file,
# `ALTER CLUSTER` rotates the secret, and another async insert then calls `getDirectoryQueue` so the
# queue drops the old pool while the batch is still paused. The rest of that batch must still land,
# and the file queued during the pause must land with the new secret.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_sql_cluster_05631"
data="data_05631"
dist="dist_05631"
failpoint="distributed_async_insert_batch_pause_after_first_file"
flush_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.flush.log"
wait_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.wait.log"
flush_pid=""
wait_pid=""

cleanup()
{
    # Unblock a flush that is still paused, then wait for both clients before dropping the table.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 || :
    if [[ -n "${wait_pid}" ]]; then
        wait "${wait_pid}" >/dev/null 2>&1 || :
    fi
    if [[ -n "${flush_pid}" ]]; then
        wait "${flush_pid}" >/dev/null 2>&1 || :
    fi
    $CLICKHOUSE_CLIENT --query "SYSTEM START DISTRIBUTED SENDS ${dist}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}" >/dev/null 2>&1 || :
    rm -f "${flush_log}" "${wait_log}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}"
$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${cluster} (
    secret = 'secret_05631_1',
    SHARD (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP})
)
"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${data} (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE ${dist} AS ${data}
ENGINE = Distributed(${cluster}, currentDatabase(), ${data})
SETTINGS background_insert_batch = 1
"

# Keep both files queued until one forced flush sends them as a single batch.
# The two statements share one `insert_query`, so they belong to the same `BatchHeader`.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP DISTRIBUTED SENDS ${dist}"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 1"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 1"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${failpoint}"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}" >"${flush_log}" 2>&1 &
flush_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE" >"${wait_log}" 2>&1 &
wait_pid=$!

# `WAIT FAILPOINT` has no timeout. If the flush fails before the pause, fail here instead of hanging.
if ! wait -n "${flush_pid}" "${wait_pid}"; then
    echo "flush failed before the batch paused"
    cat "${flush_log}" "${wait_log}"
    exit 1
fi

if kill -0 "${wait_pid}" 2>/dev/null; then
    echo "flush finished before the batch paused"
    cat "${flush_log}" "${wait_log}"
    exit 1
fi
wait_pid=""

$CLICKHOUSE_CLIENT --query "
ALTER CLUSTER ${cluster} (
    secret = 'secret_05631_2',
    SHARD (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP})
)
"

# `ALTER CLUSTER` only publishes the new cluster. The queue keeps its pool until the next async
# insert calls `getDirectoryQueue` and `updatePool`. Do that while the batch is still paused, so the
# in-flight `sendBatch` is left holding the only reference to the pool it borrowed.
$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${dist} (x) SELECT 1"

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${failpoint}"

if ! wait "${flush_pid}"; then
    echo "flush failed"
    cat "${flush_log}"
    exit 1
fi
flush_pid=""

$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${data}"

# The file queued during the pause was not part of the paused batch. It is sent with the new secret.
$CLICKHOUSE_CLIENT --query "SYSTEM START DISTRIBUTED SENDS ${dist}"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${data}"
