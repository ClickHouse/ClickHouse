#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# An `INSERT ... SELECT FROM fileCluster/urlCluster` that inserts on the initiator fills the fields missing in the files
# with the `DEFAULT` expressions of the destination table, as `file` and `url` do. All nodes of the cluster are this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

FILE_DIR="${CLICKHOUSE_DATABASE}_05482"
S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05482"
CLUSTER="test_cluster_one_shard_three_replicas_localhost"

function write_files()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION $1('$2/part_1.jsonl', 'JSONEachRow', 'id UInt32') SETTINGS $3 = 1 VALUES (1), (3)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION $1('$2/part_2.jsonl', 'JSONEachRow', 'id UInt32, q UInt32, e UInt32') SETTINGS $3 = 1 VALUES (2, 5, 6)"
}

# The first file has none of the defaulted fields, the second one has all of them.
write_files file "${FILE_DIR}" engine_file_truncate_on_insert
write_files s3 "${S3_DIR}" s3_truncate_on_insert

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dst_05482;
    CREATE TABLE dst_05482 (id UInt32, q UInt32 DEFAULT 7, e UInt32 DEFAULT id * 10) ENGINE = MergeTree ORDER BY id;
"

function check()
{
    echo "--- $1 ---"
    $CLICKHOUSE_CLIENT -q "INSERT INTO dst_05482 SELECT * FROM $2"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM dst_05482 ORDER BY id"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE dst_05482"
}

check "fileCluster" "fileCluster('${CLUSTER}', '${FILE_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"

check "fileCluster, format detected, non-replicated destination" "fileCluster('${CLUSTER}', '${FILE_DIR}/part_{1,2}.jsonl')
    SETTINGS parallel_distributed_insert_select = 2"

check "urlCluster" "urlCluster('${CLUSTER}', '${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"

check "urlCluster with headers, non-replicated destination" "urlCluster('${CLUSTER}', '${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow',
    headers('X-ClickHouse-Test' = '1')) SETTINGS parallel_distributed_insert_select = 2"

check "url with parallel replicas" "url('${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = '${CLUSTER}', parallel_replicas_for_cluster_engines = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE dst_05482"
