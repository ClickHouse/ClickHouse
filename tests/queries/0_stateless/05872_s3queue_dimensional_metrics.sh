#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# Per-table dimensional metrics of `S3Queue`: the counter of processed files, the gauge with
# the time of the last poll, and the removal of the gauges when the table is dropped.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05872"
FILES=3

for i in $(seq 1 $FILES); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data_${i}.csv', format = CSV)
        SELECT number FROM numbers(2) SETTINGS s3_truncate_on_insert = 1
    "
done

# The polling timeouts are one hour, so only the cycle triggered below can process the files.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05872_s3queue',
        mode = 'unordered',
        after_processing = 'keep',
        processing_threads_num = 1,
        polling_min_timeout_ms = 3600000,
        polling_max_timeout_ms = 3600000;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM queue;
    SYSTEM REFRESH queue;
"

TABLE_FILTER="labels['database'] = currentDatabase() AND labels['table'] = 'queue'"
GAUGES="'object_storage_queue_last_poll_timestamp_seconds', 'object_storage_queue_newest_seen_object_timestamp_seconds', 'object_storage_queue_newest_committed_object_timestamp_seconds'"

# The counter is updated after the commit, which is later than the insert into `dst`, so wait for the counter itself.
# The gauge of the newest committed object is updated before it, when each file is finalized.
for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT -q "
        SELECT sum(value) >= ${FILES} FROM system.dimensional_metrics
        WHERE metric = 'object_storage_queue_processed_files_total' AND ${TABLE_FILTER}")" -eq 1 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(x) FROM dst;

    SELECT metric, value >= ${FILES}
    FROM system.dimensional_metrics
    WHERE metric = 'object_storage_queue_processed_files_total' AND ${TABLE_FILTER};

    SELECT metric, value > 1700000000 AND value <= toUnixTimestamp(now()) + 60
    FROM system.dimensional_metrics
    WHERE metric IN (${GAUGES}) AND ${TABLE_FILTER}
    ORDER BY metric;

    DROP TABLE mv SYNC;
    DROP TABLE queue SYNC;

    SELECT 'gauges after drop', count() FROM system.dimensional_metrics WHERE metric IN (${GAUGES}) AND ${TABLE_FILTER};
    SELECT 'counter after drop', count() FROM system.dimensional_metrics
    WHERE metric = 'object_storage_queue_processed_files_total' AND ${TABLE_FILTER};
"
