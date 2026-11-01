#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# An `INSERT ... SELECT FROM fileCluster/urlCluster` that inserts on the initiator must fill the fields missing in
# the files with the `DEFAULT` expressions of the destination table, the same as a plain `INSERT ... SELECT FROM file(...)`.
# All nodes of the cluster below are this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

FILE_DIR="${CLICKHOUSE_DATABASE}_05481"
S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05481"
CLUSTER="test_cluster_one_shard_three_replicas_localhost"

# The first file has none of the defaulted fields, the second one has all of them.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE_DIR}/part_1.jsonl', 'JSONEachRow', 'id UInt32') SETTINGS engine_file_truncate_on_insert = 1 VALUES (1), (3);
    INSERT INTO FUNCTION file('${FILE_DIR}/part_2.jsonl', 'JSONEachRow', 'id UInt32, q UInt32, e UInt32') SETTINGS engine_file_truncate_on_insert = 1 VALUES (2, 5, 6);
    INSERT INTO FUNCTION s3('${S3_DIR}/part_1.jsonl', 'JSONEachRow', 'id UInt32') SETTINGS s3_truncate_on_insert = 1 VALUES (1), (3);
    INSERT INTO FUNCTION s3('${S3_DIR}/part_2.jsonl', 'JSONEachRow', 'id UInt32, q UInt32, e UInt32') SETTINGS s3_truncate_on_insert = 1 VALUES (2, 5, 6);
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_05481;
    CREATE TABLE t_05481 (id UInt32, q UInt32 DEFAULT 7, e UInt32 DEFAULT id * 10) ENGINE = MergeTree ORDER BY id;
"

function check()
{
    echo "--- $1 ---"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM t_05481 ORDER BY id"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_05481"
}

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM fileCluster('${CLUSTER}', '${FILE_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
check "fileCluster"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM urlCluster('${CLUSTER}', '${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
check "urlCluster"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM fileCluster('${CLUSTER}', '${FILE_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2"
check "fileCluster, non-replicated destination"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05481"
