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
    CREATE TABLE dst_05446 (${COLUMNS}) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_05446', 'r1') ORDER BY id;
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

# An explicit structure must reach the nodes as written, positional or `key = value`. The `SELECT` list has an
# expression, so the nodes cannot take the structure from the insertion table: with `'auto'` they would infer
# `v` as `Float64` and insert '1.5' instead of '1.50'.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/typed.jsonl', 'LineAsString', 'l String') VALUES ('{\"id\":1,\"v\":1.50}')"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS typed_05446 SYNC;
    CREATE TABLE typed_05446 (id UInt32, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/typed_05446', 'r1') ORDER BY id;
"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO typed_05446 SELECT id, toString(v) FROM s3Cluster('${CLUSTER}', '${S3_DIR}/typed.jsonl', 'JSONEachRow', 'id UInt32, v String')
    SETTINGS parallel_distributed_insert_select = 2"
check typed_05446 "s3Cluster, explicit structure"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO typed_05446 SELECT id, toString(v) FROM s3Cluster('${CLUSTER}', '${S3_DIR}/typed.jsonl', format = 'JSONEachRow', structure = 'id UInt32, v String')
    SETTINGS parallel_distributed_insert_select = 2"
check typed_05446 "s3Cluster, explicit structure as key-value argument"

# The shape of the queries of object storage ClickPipes, with a `DEFAULT now64(3)` column missing in the files and
# a TTL on it: with the default values of the type the rows got `1970-01-01` and the TTL deleted them.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/clickpipes_a.json', 'LineAsString', 'l String') VALUES ('{\"document\":\"d1\"}'), ('{\"document\":\"d2\"}')"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/clickpipes_b.json', 'LineAsString', 'l String') VALUES ('{\"document\":\"d3\"}')"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS clickpipes_05446 SYNC;
    CREATE TABLE clickpipes_05446 (document String, _synced_at DateTime64(3, 'UTC') DEFAULT now64(3))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/clickpipes_05446', 'r1') ORDER BY document
    TTL toDateTime(_synced_at) + INTERVAL 30 DAY;
"

$CLICKHOUSE_CLIENT --param_url="${S3_DIR}/clickpipes_{a,b}.json" --param_format=JSONEachRow -q "
    INSERT INTO \`${CLICKHOUSE_DATABASE}\`.\`clickpipes_05446\` (\`_synced_at\`, \`document\`)
    SELECT \`_synced_at\` AS \`_synced_at\`, \`document\` AS \`document\`
    FROM s3Cluster('${CLUSTER}', {url:String}, NOSIGN, {format:String})
    SETTINGS parallel_distributed_insert_select = 2, insert_deduplicate = 0, optimize_on_insert = 0"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE clickpipes_05446 FINAL"
echo "--- ClickPipes shape, DEFAULT now64(3) with TTL ---"
$CLICKHOUSE_CLIENT -q "SELECT count(), min(_synced_at) > now64(3) - INTERVAL 1 HOUR FROM clickpipes_05446"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dst_05446 SYNC;
    DROP TABLE typed_05446 SYNC;
    DROP TABLE clickpipes_05446 SYNC;
"
