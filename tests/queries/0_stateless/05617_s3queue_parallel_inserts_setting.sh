#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05617"
FILES=4

for i in $(seq 1 $FILES); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data_${i}.csv', format = CSV)
        SELECT ${i} SETTINGS s3_truncate_on_insert = 1
    "
done

for kind in parallel serial; do
    parallel_inserts=0
    [ "$kind" = parallel ] && parallel_inserts=1

    $CLICKHOUSE_CLIENT --send_logs_level=error -q "
        CREATE TABLE dst_${kind} (x UInt64) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE queue_${kind} (x UInt64)
        ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
        SETTINGS
            keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05617_s3queue_${kind}',
            mode = 'unordered',
            after_processing = 'keep',
            processing_threads_num = 4,
            parallel_inserts = ${parallel_inserts};
        CREATE MATERIALIZED VIEW mv_${kind} TO dst_${kind} AS SELECT x FROM queue_${kind};
    "
done

for kind in parallel serial; do
    until [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst_${kind}")" -eq $FILES ]; do
        sleep 0.5
    done
done

$CLICKHOUSE_CLIENT -q "
    SELECT table, value FROM system.s3_queue_settings
    WHERE database = currentDatabase() AND name = 'parallel_inserts'
    ORDER BY table;

    SYSTEM FLUSH LOGS text_log;

    -- Sources per INSERT of the background streaming
    SELECT DISTINCT
        extract(logger_name, '[.](queue_[a-z]+)[)]') AS table,
        extract(message, 'Using ([0-9]+) processing threads') AS threads
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND logger_name LIKE 'StorageS3Queue (' || currentDatabase() || '.queue_%'
        AND message LIKE 'Using % processing threads %'
    ORDER BY table;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_parallel SYNC;
    DROP TABLE mv_serial SYNC;
    DROP TABLE queue_parallel SYNC;
    DROP TABLE queue_serial SYNC;
"
