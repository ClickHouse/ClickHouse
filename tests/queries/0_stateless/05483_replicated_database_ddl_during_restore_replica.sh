#!/usr/bin/env bash
# Tags: zookeeper

# `SYSTEM RESTORE DATABASE REPLICA` stops and replaces the DDL worker of a `Replicated` database.
# A DDL query that was concurrently waiting in the worker used to keep a raw pointer to it
# and dereference the destroyed worker (segmentation fault). Now such a query either succeeds
# or fails with an exception, and the server stays alive.

# The racing queries fail with various errors by design; they are not interesting.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 'shard1', 'replica1')"

TIMEOUT=10

function ddl_thread()
{
    local i=0
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        i=$((i + 1))
        $CLICKHOUSE_CLIENT --query "CREATE TABLE ${DB}.t_${1}_${i} (x UInt64) ENGINE = MergeTree ORDER BY x" >/dev/null 2>&1
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${DB}.t_${1}_${i} SYNC" >/dev/null 2>&1
    done
}

function sync_thread()
{
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT --query "SYSTEM SYNC DATABASE REPLICA ${DB}" >/dev/null 2>&1
    done
}

function restore_thread()
{
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT --query "SYSTEM RESTORE DATABASE REPLICA ${DB}" >/dev/null 2>&1
    done
}

ddl_thread 1 &
ddl_thread 2 &
ddl_thread 3 &
sync_thread &
restore_thread &
restore_thread &
wait

# The server is alive and the database still accepts DDL.
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE ${DB}.t_final (x UInt64) ENGINE = MergeTree ORDER BY x" 2>/dev/null && break
    sleep 1
done
$CLICKHOUSE_CLIENT --query "EXISTS TABLE ${DB}.t_final"

$CLICKHOUSE_CLIENT --query "DROP DATABASE ${DB} SYNC"
