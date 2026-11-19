#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05768"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data_{_partition_id}.csv', format = CSV)
    PARTITION BY x SELECT number AS x FROM numbers(50) SETTINGS s3_truncate_on_insert = 1;

    CREATE TABLE queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05768_s3queue',
        mode = 'unordered',
        processing_threads_num = 2,
        parallel_inserts = 1;
"

query_id="${CLICKHOUSE_DATABASE}_05768_select"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT count(), sum(x) FROM queue WHERE NOT ignore(x, sleepEachRow(0.1))
    SETTINGS stream_like_engine_allow_direct_select = 1, max_threads = 1
" &
select_pid=$!

until [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND read_rows > 0")" -eq 1 ]; do
    if ! kill -0 $select_pid 2>/dev/null; then
        break
    fi
    sleep 0.1
done

# Re-creates the streaming tasks, the running SELECT must not be cancelled
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue MODIFY SETTING processing_threads_num = 4"
wait $select_pid

$CLICKHOUSE_CLIENT -q "DROP TABLE queue SYNC"
