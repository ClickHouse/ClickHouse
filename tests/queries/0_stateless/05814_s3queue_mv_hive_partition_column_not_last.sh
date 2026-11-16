#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: depends on S3
# Tag no-replicated-database: every replica would create the queue on the same Keeper path

# A materialized view over an S3Queue table whose Hive partition columns are not the last columns of the table.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="${CLICKHOUSE_DATABASE}_05814"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}/y=2026/m=jan/data.jsonl', format = JSONEachRow)
    SELECT 1 AS c0, 'x' AS d
    SETTINGS s3_truncate_on_insert = 1
"

$CLICKHOUSE_CLIENT --enable_object_storage_queue_hive_partitioning=1 -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.queue (y UInt16, c0 Int32, m String, d String)
    ENGINE = S3Queue(s3_conn, filename = '${S3_PATH}/y=*/m=*/data.jsonl', format = JSONEachRow)
    SETTINGS
        mode = 'unordered',
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05814_queue',
        use_hive_partitioning = 1,
        polling_min_timeout_ms = 100,
        polling_max_timeout_ms = 500
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst (c0 Int32, y UInt16, m String, d String, f String)
    ENGINE = MergeTree ORDER BY c0
"

$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv TO ${CLICKHOUSE_DATABASE}.dst
    AS SELECT c0, y, m, d, _file AS f FROM ${CLICKHOUSE_DATABASE}.queue
"

for _ in {1..120}; do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.dst")
    if [ "$count" -ge 1 ]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.dst"

$CLICKHOUSE_CLIENT -q "DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.mv SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.queue SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst SYNC"
