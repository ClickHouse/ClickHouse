#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-parallel
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node
# Tag no-parallel: the failpoint is global for the server

# A failure of the `after_processing` action is retried and then only logged as a warning.
# It must be counted in `object_storage_queue_failures_total` with the stage `after_processing`,
# once for every object for which the action did not complete.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05873"
FILES=2

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT object_storage_queue_fail_delete"
}
trap cleanup EXIT

for i in $(seq 1 $FILES); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data_${i}.csv', format = CSV)
        SELECT ${i} SETTINGS s3_truncate_on_insert = 1
    "
done

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT object_storage_queue_fail_delete"

# The polling timeouts are one hour, so only the cycle triggered below can process the files.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05873_s3queue',
        mode = 'unordered',
        after_processing = 'delete',
        after_processing_retries = 0,
        processing_threads_num = 1,
        polling_min_timeout_ms = 3600000,
        polling_max_timeout_ms = 3600000;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM queue;
    SYSTEM REFRESH queue;
"

FAILURES_FILTER="metric = 'object_storage_queue_failures_total'
    AND labels['database'] = currentDatabase() AND labels['table'] = 'queue'
    AND labels['stage'] = 'after_processing' AND labels['error_code'] = 'FAULT_INJECTED'"

for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT sum(value) >= ${FILES} FROM system.dimensional_metrics WHERE ${FAILURES_FILTER}")" -eq 1 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT sum(value) >= ${FILES} FROM system.dimensional_metrics WHERE ${FAILURES_FILTER};
    DROP TABLE mv SYNC;
    DROP TABLE queue SYNC;
"
