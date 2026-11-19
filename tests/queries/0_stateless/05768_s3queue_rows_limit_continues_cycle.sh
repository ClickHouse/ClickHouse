#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# With the file-count commit limit disabled and a row limit of one row, every batch has one file.
# A batch which processed files must continue the consumption cycle instead of ending it
# and waiting for the next poll, so one cycle (triggered by `SYSTEM REFRESH`) drains the whole backlog.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05768"
FILES=5

for i in $(seq 1 $FILES); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data_${i}.csv', format = CSV)
        SELECT ${i} SETTINGS s3_truncate_on_insert = 1
    "
done

# The polling timeouts are one hour, so only the cycle triggered below can process the files.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05768_s3queue',
        mode = 'unordered',
        after_processing = 'keep',
        processing_threads_num = 1,
        max_processed_files_before_commit = 0,
        max_processed_rows_before_commit = 1,
        polling_min_timeout_ms = 3600000,
        polling_max_timeout_ms = 3600000;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM queue;
    SYSTEM REFRESH queue;
"

for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst")" -eq $FILES ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(x) FROM dst;
    DROP TABLE mv SYNC;
    DROP TABLE queue SYNC;
"
