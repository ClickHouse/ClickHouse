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

# Many files at once, so that background inserts are still running when the next `ALTER` re-creates the streaming tasks.
function upload_backlog()
{
    local from=$1
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/backlog_{_partition_id}.csv', format = CSV)
        PARTITION BY x SELECT number AS x FROM numbers(${from}, 300) SETTINGS s3_truncate_on_insert = 1
    "
}

function wait_rows()
{
    local rows=$1
    until [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst")" -ge "$rows" ]; do
        sleep 0.5
    done
    # Every file is inserted exactly once, also the ones processed while the streaming tasks were re-created.
    $CLICKHOUSE_CLIENT -q "SELECT count() = ${rows} AND uniqExact(x) = ${rows} FROM dst"
}

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05618_s3queue',
        mode = 'unordered',
        after_processing = 'keep',
        processing_threads_num = 4,
        polling_min_timeout_ms = 100,
        polling_max_timeout_ms = 1000,
        polling_backoff_ms = 100;
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

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE queue MODIFY SETTING processing_threads_num = 0; -- { serverError BAD_ARGUMENTS }
    SELECT value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'processing_threads_num';
"

# Changes the number of streaming tasks while the backlog is being processed
upload_backlog 1000
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE queue MODIFY SETTING processing_threads_num = 8;
    SELECT value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'processing_threads_num';
"

upload 5
upload 6
wait_rows 306

# Back to a single streaming task while the backlog is being processed
upload_backlog 2000
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE queue MODIFY SETTING parallel_inserts = 0;
    SELECT value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND table = 'queue' AND name = 'parallel_inserts';
"

upload 7
upload 8
wait_rows 608

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE queue SYNC;
"
