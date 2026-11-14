#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05618"

function upload()
{
    local i=$1
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data_${i}.csv', format = CSV)
        SELECT ${i} SETTINGS s3_truncate_on_insert = 1
    "
}

function wait_rows()
{
    local rows=$1
    until [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst")" -eq "$rows" ]; do
        sleep 0.5
    done
}

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05618_s3queue',
        mode = 'unordered',
        after_processing = 'keep',
        processing_threads_num = 4;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM queue;
"

upload 1
upload 2
wait_rows 2

# With attached materialized view
$CLICKHOUSE_CLIENT -q "
    SELECT alterable FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'parallel_inserts';

    ALTER TABLE queue MODIFY SETTING parallel_inserts = 1;

    SELECT create_table_query LIKE '%parallel_inserts = 1%' FROM system.tables
    WHERE database = currentDatabase() AND name = 'queue';
    SELECT value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'parallel_inserts';
"

upload 3
upload 4
wait_rows 4

# Changes the number of streaming tasks
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE queue MODIFY SETTING processing_threads_num = 8;
    SELECT value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'processing_threads_num';
"

upload 5
upload 6
wait_rows 6

# Back to a single streaming task
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE queue MODIFY SETTING parallel_inserts = 0;
    SELECT value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'parallel_inserts';
"

upload 7
upload 8
wait_rows 8

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE queue SYNC;
"
