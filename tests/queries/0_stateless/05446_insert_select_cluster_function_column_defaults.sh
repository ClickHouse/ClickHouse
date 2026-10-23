#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: needs MinIO and a Replicated table

# A distributed `INSERT ... SELECT FROM <cluster function>` must fill the fields missing in the files with the
# `DEFAULT` expressions of the destination table, the same as a plain `INSERT ... SELECT FROM s3(...)`.
#
# The forwarded query used to carry the structure of the source storage, which has names and types only, in
# place of `'auto'`. The nodes then did not take the structure from the insertion table and filled the missing
# fields with the default values of their types (0, '', 1970-01-01).
#
# All nodes of the clusters below are this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05446"

# The first file has none of the defaulted fields, the second one has all of them.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3('${S3_DIR}/part_1.jsonl', 'JSONEachRow', 'id UInt32') VALUES (1)"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3('${S3_DIR}/part_2.jsonl', 'JSONEachRow', 'id UInt32, d Date, q UInt32, c String, e UInt32')
    VALUES (2, '2001-02-03', 5, 'x', 6)"

COLUMNS="id UInt32, d Date DEFAULT '2000-01-01', q UInt32 DEFAULT 7, c String DEFAULT 'unknown', e UInt32 DEFAULT id * 10"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dst_05446 SYNC;
    DROP TABLE IF EXISTS dist_05446;
    DROP TABLE IF EXISTS local_05446;
    CREATE TABLE dst_05446 (${COLUMNS}) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_05446', 'r1') ORDER BY id;
    CREATE TABLE local_05446 (${COLUMNS}) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dist_05446 AS local_05446 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05446, rand());
"

S3_GLOB="${S3_DIR}/part_{1,2}.jsonl"
CLUSTER="test_cluster_one_shard_three_replicas_localhost"
PARALLEL_REPLICAS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = '${CLUSTER}', parallel_replicas_for_cluster_engines = 1"

function check()
{
    local table=$1
    echo "--- $2 ---"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM ${table} ORDER BY id"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ${table}"
}

# The shape of the queries of object storage ClickPipes.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO dst_05446 (id, d, q, c, e) SELECT id, d, q, c, e FROM s3Cluster('${CLUSTER}', '${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2"
check dst_05446 "s3Cluster, column list"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dst_05446 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2"
check dst_05446 "s3Cluster, asterisk"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dst_05446 SELECT * FROM urlCluster('${CLUSTER}', '${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2"
check dst_05446 "urlCluster"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dst_05446 SELECT * FROM s3('${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, ${PARALLEL_REPLICAS}"
check dst_05446 "s3 with parallel replicas"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dist_05446 SELECT * FROM s3('${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, ${PARALLEL_REPLICAS}"
check local_05446 "s3 with parallel replicas into Distributed"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dst_05446 SYNC;
    DROP TABLE dist_05446;
    DROP TABLE local_05446;
"
