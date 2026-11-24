#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: checks ReplicatedMergeTreeSink, SharedMergeTree has its own sink.

# An async insert must not be dropped as a duplicate when the deduplication cache of a replica
# still holds a hash that was removed from Keeper by TRUNCATE on another replica.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--async_insert 1 --wait_for_async_insert 1 --deduplicate_insert enable"
VALUES="('a', 1), ('b', 2), ('c', 3)"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t2 SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t1 (k String, v Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05872/t', 'r1') ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t2 (k String, v Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05872/t', 'r2') ORDER BY tuple()"

function flush_events()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, asynchronous_insert_log"
    $CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['AsyncInsertCacheHits'], ProfileEvents['AsyncInsertCacheStaleHits'], ProfileEvents['DuplicatedAsyncInserts']
    FROM system.query_log
    -- AND current_database = currentDatabase() -- Just to silence style check: the flush is scoped via flush_query_id below.
    WHERE type = 'QueryFinish' AND query_id IN (
        SELECT flush_query_id FROM system.asynchronous_insert_log
        WHERE database = currentDatabase() AND query_id = '$1'
    )"
}

$CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO t1 VALUES $VALUES"

# Repeat the duplicate insert into t2 until its cache holds the hash.
warm=0
for i in {1..50}; do
    query_id="${CLICKHOUSE_DATABASE}_warm_${i}"
    $CLICKHOUSE_CLIENT $SETTINGS --query_id "$query_id" -q "INSERT INTO t2 VALUES $VALUES"
    if [ "$(flush_events "$query_id" | cut -f1)" == "1" ]; then
        warm=1
        break
    fi
done
[ "$warm" == "1" ] || echo "The deduplication cache of t2 was not warmed up"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t1"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t2"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t2"

query_id="${CLICKHOUSE_DATABASE}_after_truncate"
$CLICKHOUSE_CLIENT $SETTINGS --query_id "$query_id" -q "INSERT INTO t2 VALUES $VALUES"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t2"
flush_events "$query_id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t2 SYNC"
