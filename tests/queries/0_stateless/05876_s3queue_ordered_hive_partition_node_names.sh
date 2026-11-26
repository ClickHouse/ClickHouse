#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, zookeeper
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# An ordered queue with hive partitioning keeps the last processed file of every partition in a Keeper node.
# `date=2025-01-01_/city=x` and `date=2025-01-01/_city=x` must not share it, also when it was written by an older version.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function upload()
{
    local path=$1 id=$2
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/${path}', format = CSV)
        SELECT ${id} SETTINGS s3_truncate_on_insert = 1
    "
}

function create_queue()
{
    local queue=$1
    $CLICKHOUSE_CLIENT --send_logs_level=error -q "
        CREATE TABLE dst_${queue} (id UInt32) ENGINE = MergeTree ORDER BY id;
        CREATE TABLE ${queue} (id UInt32)
        ENGINE = S3Queue('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_05876_${queue}/*/*/*.csv', 'CSV')
        SETTINGS
            keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05876_${queue}',
            mode = 'ordered',
            partitioning_mode = 'hive',
            processing_threads_num = 1,
            after_processing = 'keep',
            polling_min_timeout_ms = 300,
            polling_max_timeout_ms = 600;
    "
}

function create_mv()
{
    local queue=$1
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv_${queue} TO dst_${queue} AS SELECT id FROM ${queue}"
}

function flush()
{
    local queue=$1 path=$2
    $CLICKHOUSE_CLIENT --max_execution_time 120 -q "SYSTEM FLUSH OBJECT STORAGE QUEUE ${queue} PATH '${CLICKHOUSE_DATABASE}_05876_${queue}/${path}'"
}

function show()
{
    local queue=$1
    $CLICKHOUSE_CLIENT -q "
        SELECT groupArray(id) FROM (SELECT id FROM dst_${queue} ORDER BY id);
        SELECT name, replaceOne(value, '${CLICKHOUSE_DATABASE}_05876_${queue}/', '') FROM system.zookeeper
        WHERE path = '/clickhouse/${CLICKHOUSE_DATABASE}/05876_${queue}/processed' ORDER BY name;
        DROP TABLE mv_${queue} SYNC;
        DROP TABLE ${queue} SYNC;
    "
}

echo "Two partitions with the same old node name"
P="${CLICKHOUSE_DATABASE}_05876_qa"
upload "${P}/date=2025-01-01_/city=x/b.csv" 2
create_queue qa
create_mv qa
flush qa "date=2025-01-01_/city=x/b.csv"
upload "${P}/date=2025-01-01/_city=x/a.csv" 1
upload "${P}/date=2025-01-02/_city=x/a.csv" 3
upload "${P}/date=2025-01-03/city=z/a.csv" 4
flush qa "date=2025-01-02/_city=x/a.csv"
flush qa "date=2025-01-03/city=z/a.csv"
flush qa "date=2025-01-01/_city=x/a.csv"
show qa

echo "Keeper nodes left by an older version"
P="${CLICKHOUSE_DATABASE}_05876_qb"
KB="/clickhouse/${CLICKHOUSE_DATABASE}/05876_qb"
create_queue qb
$CLICKHOUSE_CLIENT -q "
    INSERT INTO system.zookeeper (path, name, value) SELECT '${KB}', 'processed',
        '{\"file_path\":\"${P}/date=2025-01-03/city=z/b.csv\",\"last_processed_timestamp\":0,\"last_exception\":\"\",\"retries\":0,\"processor_id\":\"\"}';
    INSERT INTO system.zookeeper (path, name, value) SELECT '${KB}/processed', 'date=2025-01-01___city=x', '${P}/date=2025-01-01_/city=x/b.csv';
    INSERT INTO system.zookeeper (path, name, value) SELECT '${KB}/processed', 'date=2025-01-03_city=z', '${P}/date=2025-01-03/city=z/b.csv';
"
upload "${P}/date=2025-01-01_/city=x/a.csv" 10
upload "${P}/date=2025-01-01_/city=x/c.csv" 11
upload "${P}/date=2025-01-01/_city=x/a.csv" 12
upload "${P}/date=2025-01-03/city=z/a.csv" 13
upload "${P}/date=2025-01-03/city=z/c.csv" 14
# Without a materialized view only a file that is already processed can be flushed.
flush qb "date=2025-01-01_/city=x/a.csv"
flush qb "date=2025-01-03/city=z/a.csv"
create_mv qb
flush qb "date=2025-01-01_/city=x/c.csv"
flush qb "date=2025-01-01/_city=x/a.csv"
flush qb "date=2025-01-03/city=z/c.csv"
show qb

$CLICKHOUSE_CLIENT -q "DROP TABLE dst_qa; DROP TABLE dst_qb;"
