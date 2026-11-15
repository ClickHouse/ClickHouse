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

# The same query with a `headers` argument, whose value does not matter: with it, the arguments for the nodes are
# edited by another branch of `TableFunctionURL::updateStructureAndFormatArgumentsIfNeeded`, which takes `headers` out,
# adds the format and structure, and puts `headers` back.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM urlCluster('${CLUSTER}', '${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow', headers('X-ClickHouse-Test' = '1'))
    SETTINGS parallel_distributed_insert_select = 0"
check "urlCluster with headers"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM url('${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = '${CLUSTER}',
        parallel_replicas_for_cluster_engines = 1, parallel_distributed_insert_select = 0"
check "url converted to urlCluster"

# The conversion has to happen, otherwise the case above reads on the initiator only and proves nothing.
echo "--- url converted to urlCluster reads from the cluster ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN SELECT * FROM url('${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = '${CLUSTER}',
        parallel_replicas_for_cluster_engines = 1" | grep -c ReadFromCluster

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM fileCluster('${CLUSTER}', '${FILE_DIR}/part_{1,2}.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2"
check "fileCluster, non-replicated destination"

# The nodes must not receive the inferred values of the keys of a named collection that are \`NOT OVERRIDABLE\`.
NC="nc_${CLICKHOUSE_DATABASE}_05481"
$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${NC}_structure;
    DROP NAMED COLLECTION IF EXISTS ${NC}_format;
    CREATE NAMED COLLECTION ${NC}_structure AS url = '${S3_DIR}/part_{1,2}.jsonl', format = 'JSONEachRow', structure = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_format AS url = '${S3_DIR}/part_{1,2}.jsonl', format = 'auto' NOT OVERRIDABLE, structure = 'id UInt32';
"

echo "--- urlCluster, structure not overridable ---"
$CLICKHOUSE_CLIENT -q "SELECT * FROM urlCluster('${CLUSTER}', ${NC}_structure) ORDER BY id"
echo "--- urlCluster, format not overridable ---"
$CLICKHOUSE_CLIENT -q "SELECT * FROM urlCluster('${CLUSTER}', ${NC}_format) ORDER BY id"

# The nodes infer the structure themselves, so the initiator must not take it from the destination either.
echo "--- urlCluster, structure not overridable, INSERT ---"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM urlCluster('${CLUSTER}', ${NC}_structure)
    SETTINGS parallel_distributed_insert_select = 0" 2>&1 | grep -o -m1 "NUMBER_OF_COLUMNS_DOESNT_MATCH"
echo "--- url converted to urlCluster, structure not overridable, INSERT ---"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_05481 SELECT * FROM url(${NC}_structure)
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = '${CLUSTER}',
        parallel_replicas_for_cluster_engines = 1, parallel_distributed_insert_select = 0" 2>&1 | grep -o -m1 "NUMBER_OF_COLUMNS_DOESNT_MATCH"

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION ${NC}_structure;
    DROP NAMED COLLECTION ${NC}_format;
    DROP TABLE t_05481;
"

# A plain `SELECT` goes the same way: the nodes must receive the structure and format the initiator resolved instead of
# inferring the schema again. Every forwarded query must name the format and a structure after it.
for function in "fileCluster('${CLUSTER}', '${FILE_DIR}/part_{1,2}.jsonl', 'JSONEachRow')" \
    "urlCluster('${CLUSTER}', '${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')"
do
    query_id="05481_select_${CLICKHOUSE_DATABASE}_${RANDOM}"
    $CLICKHOUSE_CLIENT --query_id "${query_id}" -q "SELECT * FROM ${function} FORMAT Null"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    echo "--- forwarded SELECT from ${function%%(*} ---"
    $CLICKHOUSE_CLIENT -q "
        SELECT count() > 0, countIf(query NOT LIKE '%''JSONEachRow'', ''%')
        FROM system.query_log
        WHERE event_date >= yesterday() AND NOT is_initial_query AND type = 'QueryFinish'
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}')"
done
