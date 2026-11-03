#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO, and the interserver secret needs the SSL library

# Cluster table functions over a named collection with `format` or `structure` = 'auto' NOT OVERRIDABLE.
# The nodes of a cluster with a secret accept the inferred values from the initiator, so an INSERT gets the `DEFAULT`s
# of the destination. The nodes of a cluster without a secret are not sent them and infer them. A value written by the
# user, or forwarded without the secret or by a function that is not a cluster function, stays locked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05493"
SECRET="test_cluster_interserver_secret"
PLAIN="test_cluster_one_shard_three_replicas_localhost"
NC="nc_${CLICKHOUSE_DATABASE}_05493"
QID="${CLICKHOUSE_DATABASE}_05493"
COLLECTIONS="url_structure url_structure_rev url_format s3_structure s3_format s3_structure_json s3_write url_late s3_late"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP VIEW IF EXISTS v_url_05493; DROP VIEW IF EXISTS v_s3_05493; DROP TABLE IF EXISTS dst_05493"
    for nc in $COLLECTIONS; do
        $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}_${nc}"
    done
}
trap cleanup EXIT

# Prints the result of a query, or the name of its error.
function query()
{
    local output
    if output=$($CLICKHOUSE_CLIENT "$@" 2>&1); then
        [ -z "$output" ] || echo "$output"
    else
        echo "$output" | grep -oE '\([A-Z_]+\)' | tail -1 | tr -d '()'
    fi
}

# Prints only the "Override not allowed" part of the error of a query that must fail.
function expect_locked()
{
    local key=$1 output
    shift
    if output=$($CLICKHOUSE_CLIENT "$@" 2>&1); then
        echo "Expected a refusal: $*"
    fi
    echo "$output" | grep -o "Override not allowed for '$key'" | head -1
}

function write_file()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/$1', '$2', '$3') SETTINGS s3_truncate_on_insert = 1 VALUES $4"
}

FULL="id UInt32, q UInt32, e UInt32"
# Parquet infers `q` and `e` as not `Nullable`, so a row of a file without them gets 0 unless the structure carries the `DEFAULT`s.
write_file p/part_1.parquet Parquet "${FULL}" "(2, 5, 6)"
write_file p/part_2.parquet Parquet "id UInt32" "(1), (3)"
# The first file has none of the defaulted fields.
write_file r/part_1.parquet Parquet "id UInt32" "(1), (3)"
write_file r/part_2.parquet Parquet "${FULL}" "(2, 5, 6)"
write_file j/part_1.jsonl JSONEachRow "${FULL}" "(2, 5, 6)"
write_file j/part_2.jsonl JSONEachRow "id UInt32" "(1), (3)"

cleanup
$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${NC}_url_structure AS url = '${S3_DIR}/p/part_{1,2}.parquet', format = 'Parquet',
        structure = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_url_structure_rev AS url = '${S3_DIR}/r/part_{1,2}.parquet', format = 'Parquet',
        structure = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_url_format AS url = '${S3_DIR}/p/part_{1,2}.parquet', format = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_s3_structure AS url = '${S3_DIR}/p/part_{1,2}.parquet', format = 'Parquet',
        structure = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_s3_format AS url = '${S3_DIR}/p/part_{1,2}.parquet', format = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_s3_structure_json AS url = '${S3_DIR}/j/part_{1,2}.jsonl', format = 'JSONEachRow',
        structure = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_s3_write AS url = '${S3_DIR}/w/out.parquet', format = 'Parquet', structure = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC}_url_late AS url = '${S3_DIR}/p/part_{1,2}.parquet', format = 'Parquet', structure = 'auto';
    CREATE NAMED COLLECTION ${NC}_s3_late AS url = '${S3_DIR}/p/part_{1,2}.parquet', format = 'Parquet', structure = 'auto';
    CREATE TABLE dst_05493 (id UInt32, q UInt32 DEFAULT 7, e UInt32 DEFAULT id * 10) ENGINE = MergeTree ORDER BY id;
"

function check()
{
    echo "--- $1 ---"
    query -q "INSERT INTO dst_05493 SELECT * FROM $2"
    query -q "SELECT * FROM dst_05493 ORDER BY id"
    query -q "TRUNCATE TABLE dst_05493"
}

function parallel_replicas()
{
    echo "enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = $2,
        cluster_for_parallel_replicas = '$1', parallel_replicas_for_cluster_engines = 1"
}

echo "=== cluster with a secret ==="
check "urlCluster, locked structure" "urlCluster('${SECRET}', ${NC}_url_structure) SETTINGS parallel_distributed_insert_select = 0"
check "urlCluster, locked structure, first file without the defaulted fields" \
    "urlCluster('${SECRET}', ${NC}_url_structure_rev) SETTINGS parallel_distributed_insert_select = 0"
check "url with parallel replicas, locked structure" \
    "url(${NC}_url_structure) SETTINGS parallel_distributed_insert_select = 0, $(parallel_replicas "${SECRET}" 2)"
echo "--- urlCluster, locked format ---"
query --query_id "${QID}_url_format" -q "SELECT id FROM urlCluster('${SECRET}', ${NC}_url_format) ORDER BY id"
check "s3Cluster, locked structure" "s3Cluster('${SECRET}', ${NC}_s3_structure) SETTINGS parallel_distributed_insert_select = 0"
check "s3 with parallel replicas, locked structure" \
    "s3(${NC}_s3_structure) SETTINGS parallel_distributed_insert_select = 0, $(parallel_replicas "${SECRET}" 2)"
echo "--- s3Cluster, locked format ---"
query --query_id "${QID}_s3_format" -q "SELECT id FROM s3Cluster('${SECRET}', ${NC}_s3_format) ORDER BY id"
echo "--- s3 with parallel replicas, locked format ---"
query --query_id "${QID}_s3_format_pr" -q "SELECT id FROM s3(${NC}_s3_format) ORDER BY id SETTINGS $(parallel_replicas "${SECRET}" 2)"
echo "--- parallel replicas read from the cluster ---"
# The rows above are the same if the cluster path is not taken.
for args in "url(${NC}_url_structure)" "s3(${NC}_s3_structure)" "s3(${NC}_s3_format)"; do
    $CLICKHOUSE_CLIENT -q "EXPLAIN SELECT id FROM ${args} SETTINGS $(parallel_replicas "${SECRET}" 2)" | grep -c ReadFromCluster
done

echo "=== cluster without a secret ==="
for key in format structure; do
    echo "--- s3Cluster, locked ${key} ---"
    query --query_id "${QID}_plain_${key}" -q "SELECT id FROM s3Cluster('${PLAIN}', ${NC}_s3_${key}) ORDER BY id"
done
# The nodes infer `Nullable` fields, so `insert_null_as_default` applies the `DEFAULT`s.
check "s3Cluster, locked structure, JSONEachRow" \
    "s3Cluster('${PLAIN}', ${NC}_s3_structure_json) SETTINGS parallel_distributed_insert_select = 0"
check "s3 with parallel replicas, locked structure, JSONEachRow" \
    "s3(${NC}_s3_structure_json) SETTINGS parallel_distributed_insert_select = 0, $(parallel_replicas "${PLAIN}" 3)"
echo "--- parallel replicas read from the cluster, without a secret ---"
$CLICKHOUSE_CLIENT -q "EXPLAIN SELECT id FROM s3(${NC}_s3_structure_json) SETTINGS $(parallel_replicas "${PLAIN}" 3)" | grep -c ReadFromCluster
echo "--- INSERT INTO FUNCTION s3Cluster, locked structure ---"
query -q "INSERT INTO FUNCTION s3Cluster('${PLAIN}', ${NC}_s3_write) SELECT number AS id, number * 2 AS v FROM numbers(3)
    SETTINGS s3_truncate_on_insert = 1"
query -q "SELECT * FROM s3('${S3_DIR}/w/out.parquet') ORDER BY id"
echo "--- the inferred format is sent to the nodes through the secret only ---"
# The rows of the locked format cases are the same if the nodes infer the format from the file extension.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
for id in url_format s3_format s3_format_pr plain_format; do
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(query LIKE '%format = _Parquet_%') > 0, count() > 0 FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryStart' AND NOT is_initial_query
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${QID}_${id}')"
done

echo "=== the lock binds a written value ==="
expect_locked structure -q "SELECT * FROM urlCluster('${SECRET}', ${NC}_url_structure, structure = 'id UInt32')"
expect_locked structure -q "SELECT * FROM s3Cluster('${SECRET}', ${NC}_s3_structure, structure = 'id UInt32')"
echo "--- in a secondary query without the secret ---"
expect_locked structure --query_kind secondary_query -q "SELECT * FROM urlCluster('${SECRET}', ${NC}_url_structure, structure = 'id UInt32')"
expect_locked structure --query_kind secondary_query -q "SELECT * FROM s3Cluster('${SECRET}', ${NC}_s3_structure, structure = 'id UInt32')"
echo "--- in a secondary query that reports the interserver interface without the secret ---"
python3 "$CUR_DIR"/05493_cluster_functions_locked_auto_interserver_secret.python "${SECRET}" "${NC}"
echo "--- in a function that is not a cluster function, forwarded with the secret ---"
$CLICKHOUSE_CLIENT -q "
    CREATE VIEW v_url_05493 AS SELECT * FROM url(${NC}_url_late, structure = 'id UInt32');
    CREATE VIEW v_s3_05493 AS SELECT * FROM s3(${NC}_s3_late, structure = 'id UInt32');
"
for view in v_url_05493 v_s3_05493; do
    query -q "SELECT count() FROM cluster('${SECRET}', currentDatabase(), ${view}) SETTINGS prefer_localhost_replica = 0"
done
$CLICKHOUSE_CLIENT -q "
    ALTER NAMED COLLECTION ${NC}_url_late SET structure = 'auto' NOT OVERRIDABLE;
    ALTER NAMED COLLECTION ${NC}_s3_late SET structure = 'auto' NOT OVERRIDABLE;
"
for view in v_url_05493 v_s3_05493; do
    expect_locked structure -q "SELECT count() FROM cluster('${SECRET}', currentDatabase(), ${view}) SETTINGS prefer_localhost_replica = 0"
done
