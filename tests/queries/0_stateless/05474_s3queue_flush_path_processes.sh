#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="${CLICKHOUSE_DATABASE}_05474"
KEEPER_PATH="/clickhouse/${CLICKHOUSE_DATABASE}/05474_queue"

function write_and_flush()
{
    $CLICKHOUSE_CLIENT "${@:2}" -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/${S3_DIR}/$1.csv', format = CSV) SELECT '$1' SETTINGS s3_truncate_on_insert = 1;
        SYSTEM FLUSH OBJECT STORAGE QUEUE queue PATH '${S3_DIR}/$1.csv';
        SELECT * FROM dst ORDER BY ALL;
    "
}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE queue (s String)
    ENGINE = S3Queue('http://localhost:11111/test/${S3_DIR}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '${KEEPER_PATH}',
        mode = 'ordered',
        partitioning_mode = 'explicit',
        bucketing_mode = 'partition',
        buckets = 2,
        polling_min_timeout_ms = 3600000,
        polling_max_timeout_ms = 3600000;
    ALTER TABLE queue ADD PARTITION 'a_';
    CREATE TABLE dst (s String) ENGINE = MergeTree ORDER BY s;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT s FROM queue;
"

write_and_flush a_1
write_and_flush a_2
write_and_flush b_1 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

$CLICKHOUSE_KEEPER_CLIENT -q "create '${KEEPER_PATH}/buckets/0/lock' 'test'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '${KEEPER_PATH}/buckets/1/lock' 'test'"
write_and_flush a_3 --max_execution_time 2 2>&1 | grep -o -m1 "TIMEOUT_EXCEEDED"
$CLICKHOUSE_KEEPER_CLIENT -q "rm '${KEEPER_PATH}/buckets/0/lock'"
$CLICKHOUSE_KEEPER_CLIENT -q "rm '${KEEPER_PATH}/buckets/1/lock'"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH OBJECT STORAGE QUEUE queue PATH '${S3_DIR}/a_3.csv';
    SELECT * FROM dst ORDER BY ALL;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv;
    DROP TABLE queue;
    DROP TABLE dst;
"
