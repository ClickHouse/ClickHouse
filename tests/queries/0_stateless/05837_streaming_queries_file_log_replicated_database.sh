#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper, no-replicated-database, no-ordinary-database
# no-replicated-database: this test explicitly creates a Replicated database.

# The cursor of a streaming read of a `FileLog` table has positions in the files of one server, so an incremental
# refresh from it cannot be coordinated between the replicas of a `Replicated` database, which share one cursor.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$dir"
mkdir -p "$dir"
printf '1\n2\n' > "$dir/a.log"

db="${CLICKHOUSE_DATABASE}_r"
client="$STREAMING_CLIENT --distributed_ddl_output_mode=none"

$client -q "
    CREATE DATABASE ${db} ENGINE = Replicated('/test/{database}/streaming_queries_file_log', 'shard1', 'replica1');
    CREATE TABLE ${db}.file_log (k UInt64) ENGINE = FileLog('$dir', 'CSV');
    CREATE TABLE ${db}.tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
"

echo "--- a coordinated refresh is rejected"
$client -q "CREATE MATERIALIZED VIEW ${db}.mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO ${db}.tgt EMPTY AS SELECT k FROM ${db}.file_log" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

echo "--- a refresh on every replica is allowed"
$client -q "
    CREATE MATERIALIZED VIEW ${db}.mv REFRESH EVERY 10 YEAR SETTINGS all_replicas = 1 APPEND INCREMENTAL TO ${db}.tgt EMPTY AS SELECT k FROM ${db}.file_log;
    SYSTEM REFRESH VIEW ${db}.mv;
    SYSTEM WAIT VIEW ${db}.mv;
    SELECT count(), sum(k) FROM ${db}.tgt;
"

$client -q "DROP DATABASE ${db} SYNC"
rm -rf "$dir"
