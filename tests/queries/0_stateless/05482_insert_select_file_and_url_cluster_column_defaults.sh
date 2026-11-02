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
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION $1('$2/$4.jsonl', 'JSONEachRow', 'id UInt32') SETTINGS $3 = 1 VALUES (1), (3)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION $1('$2/$5.jsonl', 'JSONEachRow', 'id UInt32, q UInt32, e UInt32') SETTINGS $3 = 1 VALUES (2, 5, 6)"
}

# The first file has none of the defaulted fields, the second one has all of them.
write_files file "${FILE_DIR}" engine_file_truncate_on_insert part_1 part_2
write_files s3 "${S3_DIR}" s3_truncate_on_insert part_1 part_2
# The same rows, the file that has all of them first.
write_files s3 "${S3_DIR}/full_first" s3_truncate_on_insert part_2 part_1

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

PARALLEL_REPLICAS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = '${CLUSTER}', parallel_replicas_for_cluster_engines = 1"

check "url with parallel replicas" "url('${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0, ${PARALLEL_REPLICAS}"

# A named collection key locked as 'auto' is not sent to the nodes, which would refuse it as an override. They infer it,
# and so does the initiator for a locked structure, which is therefore inferred from files that start with every field.
# The structure sent for a locked format carries the `DEFAULT`s.
NC="nc_${CLICKHOUSE_DATABASE}_05482"
trap '$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}_format; DROP NAMED COLLECTION IF EXISTS ${NC}_structure"' EXIT
$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${NC}_format;
    DROP NAMED COLLECTION IF EXISTS ${NC}_structure;
    CREATE NAMED COLLECTION ${NC}_format AS url = '${S3_DIR}/part_{1,2}.jsonl', format = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_structure AS url = '${S3_DIR}/full_first/part_{1,2}.jsonl', format = 'JSONEachRow',
        structure = 'auto' NOT OVERRIDABLE;
"

echo "--- url with parallel replicas reads from the cluster ---"
for args in "'${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow'" "${NC}_format" "${NC}_structure"; do
    $CLICKHOUSE_CLIENT -q "EXPLAIN SELECT id FROM url(${args}) SETTINGS ${PARALLEL_REPLICAS}" | grep -c ReadFromCluster
done

for key in format structure; do
    echo "--- locked ${key}: SELECT from urlCluster ---"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM urlCluster('${CLUSTER}', ${NC}_${key}) ORDER BY id"
    echo "--- locked ${key}: SELECT from url with parallel replicas ---"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM url(${NC}_${key}) ORDER BY id SETTINGS ${PARALLEL_REPLICAS}"
    check "locked ${key}: urlCluster" "urlCluster('${CLUSTER}', ${NC}_${key}) SETTINGS parallel_distributed_insert_select = 0"
    check "locked ${key}: url with parallel replicas" "url(${NC}_${key})
        SETTINGS parallel_distributed_insert_select = 0, ${PARALLEL_REPLICAS}"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE dst_05482"
