#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_05473"
KEEPER_PATH="/clickhouse/${CLICKHOUSE_DATABASE}/05473_queue"

function write()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, url = '${S3_URL}/$1.csv', format = CSV) SELECT '$1' SETTINGS s3_truncate_on_insert = 1"
}

function consume()
{
    $CLICKHOUSE_CLIENT "$@" -q "SELECT * FROM queue ORDER BY ALL SETTINGS stream_like_engine_allow_direct_select = 1"
}

write a_1
write b_1
write c_1

$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE queue (s String)
    ENGINE = S3Queue('${S3_URL}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '${KEEPER_PATH}',
        mode = 'ordered',
        partitioning_mode = 'explicit',
        bucketing_mode = 'partition',
        buckets = 2,
        after_processing = 'keep',
        commit_on_select = 1;
    ALTER TABLE queue ADD PARTITION 'a_';
    ALTER TABLE queue ADD PARTITION 'b_';
    ALTER TABLE queue ADD PARTITION 'a_';
    ALTER TABLE queue FORGET PARTITION 'd_';
    ALTER TABLE queue ADD PARTITION 'a'; -- { serverError BAD_ARGUMENTS }
    SELECT name FROM system.zookeeper WHERE path = '${KEEPER_PATH}/partitions' ORDER BY name;
"

consume

write a_2
consume --query_id "${CLICKHOUSE_DATABASE}_05473_second"

$CLICKHOUSE_CLIENT -n -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'listed', ProfileEvents['ObjectStorageQueueListedFiles'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_DATABASE}_05473_second' AND type = 'QueryFinish';
    ALTER TABLE queue FORGET PARTITION 'a_';
    SELECT name FROM system.zookeeper WHERE path = '${KEEPER_PATH}/partitions' ORDER BY name;
    SELECT name FROM system.zookeeper WHERE path IN ('${KEEPER_PATH}/buckets/0/processed', '${KEEPER_PATH}/buckets/1/processed') ORDER BY name;
    CREATE TABLE queue_plain (s String)
    ENGINE = S3Queue('${S3_URL}/*.csv', 'CSV')
    SETTINGS keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05473_queue_plain', mode = 'ordered';
    ALTER TABLE queue_plain ADD PARTITION 'a_'; -- { serverError BAD_ARGUMENTS }
    DROP TABLE queue;
    DROP TABLE queue_plain;
"
