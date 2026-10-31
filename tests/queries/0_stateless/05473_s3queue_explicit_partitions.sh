#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_05473"

function write()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, url = '${S3_URL}/$1.csv', format = CSV) SELECT '$1' SETTINGS s3_truncate_on_insert = 1"
}

function consume()
{
    $CLICKHOUSE_CLIENT --query_id "$1" -q "SELECT * FROM queue ORDER BY ALL SETTINGS stream_like_engine_allow_direct_select = 1"
}

write a_1
write b_1
write c_1

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE queue (s String)
    ENGINE = S3Queue('${S3_URL}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05473_queue',
        mode = 'ordered',
        partitioning_mode = 'explicit',
        bucketing_mode = 'partition',
        buckets = 2,
        after_processing = 'keep',
        commit_on_select = 1
"

$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION 'a_'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION 'b_'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION 'a_'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue FORGET PARTITION 'd_'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION 'a'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION 'b_1'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION ''" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/${CLICKHOUSE_DATABASE}/05473_queue/partitions' ORDER BY name"

echo "first"
consume "${CLICKHOUSE_DATABASE}_05473_first"

write a_2
echo "second"
consume "${CLICKHOUSE_DATABASE}_05473_second"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['ObjectStorageQueueListedFiles'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_DATABASE}_05473_second' AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT -q "ALTER TABLE queue FORGET PARTITION 'a_'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue ADD PARTITION 'a_'"
echo "after forget"
consume "${CLICKHOUSE_DATABASE}_05473_third"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE queue_plain (s String)
    ENGINE = S3Queue('${S3_URL}/*.csv', 'CSV')
    SETTINGS keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05473_queue_plain', mode = 'ordered'
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue_plain ADD PARTITION 'a_'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -q "ALTER TABLE queue_plain FORGET PARTITION 'a_'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE queue_path_buckets (s String)
    ENGINE = S3Queue('${S3_URL}/*.csv', 'CSV')
    SETTINGS keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05473_queue_path_buckets', mode = 'ordered', partitioning_mode = 'explicit', buckets = 2
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT -q "DROP TABLE queue"
$CLICKHOUSE_CLIENT -q "DROP TABLE queue_plain"
