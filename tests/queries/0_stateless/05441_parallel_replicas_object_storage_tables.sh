#!/usr/bin/env bash
# Tags: long, no-fasttest
# Tag no-fasttest: Depends on S3

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PR_SETTINGS="enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_cluster_engines = 1,
    parallel_replicas_for_object_storage_tables = 1, allow_correlated_subqueries = 1"

# Prints whether the query reads with parallel replicas (ReadFromCluster in the plan), then whether its result
# with them equals the result without them, then the result.
# One client runs all three queries: client startup dominates the run time under sanitizers.
function check()
{
    local query="$1"
    local settings="${PR_SETTINGS}${2:+, $2}"
    local separator="check_output_separator"
    local output with_pr without_pr
    output=$($CLICKHOUSE_CLIENT -q "
        EXPLAIN $query SETTINGS $settings;
        SELECT '$separator';
        $query SETTINGS $settings;
        SELECT '$separator';
        $query SETTINGS enable_parallel_replicas = 0;
    ")
    awk -v s="$separator" '$0 == s { exit } /ReadFromCluster/ { ++n } END { print n + 0 }' <<< "$output"
    with_pr=$(awk -v s="$separator" '$0 == s { ++n; next } n == 1' <<< "$output")
    without_pr=$(awk -v s="$separator" '$0 == s { ++n; next } n == 2' <<< "$output")
    [ "$with_pr" = "$without_pr" ] && echo "same result" || echo "different result: '$with_pr' vs '$without_pr'"
    echo "$with_pr"
}

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/f_{_partition_id}.parquet', format = 'Parquet')
    PARTITION BY a % 6 SELECT number AS a, toString(number) AS b FROM numbers(600) SETTINGS s3_truncate_on_insert = 1;
    CREATE TABLE t (a UInt64, b String) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/*.parquet', format = 'Parquet');
    CREATE TABLE ice ENGINE = IcebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
"

echo "-- S3"
check "SELECT sum(a), count() FROM t WHERE a > 10"
check "SELECT b, a FROM t ARRAY JOIN [1, 2] AS x WHERE a > 595 AND x = 1 ORDER BY a"

echo "-- IcebergS3 with deletes"
check "SELECT sum(id), count() FROM ice"

echo "-- disabled"
check "SELECT sum(a) FROM t" "parallel_replicas_for_object_storage_tables = 0"
check "SELECT sum(a) FROM t" "parallel_replicas_for_cluster_engines = 0"
check "SELECT sum(a) FROM t" "max_parallel_replicas = 1, parallel_replicas_prefer_local_replica = 0"

echo "-- JOIN"
check "SELECT count() FROM t AS x JOIN t AS y USING (a)"
check "SELECT count() FROM t AS x, t AS y WHERE x.a = y.a"

echo "-- row policy"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${CLICKHOUSE_DATABASE}_rp ON t USING a < 100 TO CURRENT_USER"
check "SELECT sum(a), count() FROM t"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${CLICKHOUSE_DATABASE}_rp ON t"

echo "-- SQL SECURITY DEFINER and NONE views"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS u_${CLICKHOUSE_DATABASE};
    CREATE USER u_${CLICKHOUSE_DATABASE};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO u_${CLICKHOUSE_DATABASE};
    CREATE VIEW v DEFINER = u_${CLICKHOUSE_DATABASE} SQL SECURITY DEFINER AS SELECT a FROM t;
    CREATE VIEW vn SQL SECURITY NONE AS SELECT a FROM t;
"
check "SELECT sum(a) FROM v"
check "SELECT sum(a) FROM vn"

echo "-- PREWHERE"
check "SELECT sum(a) FROM t PREWHERE a > 10"

echo "-- correlated subquery"
check "SELECT count() FROM t AS x WHERE exists(SELECT 1 FROM numbers(20) AS n WHERE n.number = x.a)"

echo "-- the table read again in a subquery"
check "SELECT count() FROM t WHERE a IN (SELECT a FROM t WHERE a < 50)"
check "SELECT count() FROM t WHERE a IN (SELECT a FROM t WHERE a < 50)" "parallel_replicas_allow_in_with_subquery = 0"

# No primary key, so the IN set does not prune `mt` and the followers read it too.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO mt SELECT number FROM numbers(600);
"

echo "-- the table in a UNION subquery"
check "SELECT count() FROM t WHERE a IN (SELECT a FROM t WHERE a < 50 UNION ALL SELECT a FROM t WHERE a < 50)"
check "SELECT count() FROM mt WHERE a IN (SELECT a FROM t WHERE a < 50 UNION ALL SELECT a FROM t WHERE a < 50)" "parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1"

echo "-- another source in the query"
$CLICKHOUSE_CLIENT -q "CREATE VIEW vc AS SELECT a FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = '${CLICKHOUSE_DATABASE}/*.parquet', format = 'Parquet')"
check "SELECT count() FROM t WHERE a IN (SELECT a FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = '${CLICKHOUSE_DATABASE}/*.parquet', format = 'Parquet') WHERE a < 50)"
check "SELECT count() FROM t WHERE a IN (SELECT a FROM vc WHERE a < 50)"
check "SELECT count() FROM t WHERE a IN (SELECT a FROM mt WHERE a < 50)"
check "SELECT (SELECT count() FROM t WHERE a IN vc AND a < 50)"
# Prints the result of the query, then whether it read anything with parallel replicas.
function check_shards()
{
    local query_id="${CLICKHOUSE_DATABASE}_$1"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "$2 SETTINGS $PR_SETTINGS"
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['Shards'] > 0 FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
}

# A plain `s3` with its own SETTINGS in a subquery, a CTE, an alias, a lambda or a SQL function is still read with parallel replicas.
S3_SOURCE="s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/*.parquet', format = 'Parquet') WHERE a < 50 SETTINGS max_threads = 2"
check_shards s3_in_subquery "SELECT count() FROM t WHERE a IN (SELECT a FROM $S3_SOURCE)"
check_shards s3_in_cte "WITH c AS (SELECT a FROM $S3_SOURCE) SELECT count() FROM t WHERE a IN c"
check "WITH (SELECT max(a) FROM $S3_SOURCE) AS m SELECT count() FROM t WHERE a <= m"
check_shards s3_in_lambda "WITH x -> x IN (SELECT a FROM $S3_SOURCE) AS f SELECT count() FROM t WHERE f(a)"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${CLICKHOUSE_DATABASE}_f AS x -> x IN (SELECT a FROM $S3_SOURCE)"
check_shards s3_in_function "SELECT count() FROM t WHERE ${CLICKHOUSE_DATABASE}_f(a)"

echo "-- a subquery or a set that reads no table"
check "SELECT sum(a) FROM t WHERE a > (SELECT 1)"
check "WITH (1, 2) AS s SELECT count() FROM t WHERE a IN s"

echo "-- a server constant"
# Computed on the initiator, as when the table is read without parallel replicas.
check "SELECT hostName() = (SELECT hostName()) AS h, count() FROM t GROUP BY h"

echo "-- hive partitioning"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE th (a UInt64, p UInt8) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}_hive/', format = 'Parquet', partition_strategy = 'hive')
    PARTITION BY p;
    INSERT INTO th SELECT number, number % 3 FROM numbers(30);
"
check "SELECT p, sum(a) FROM th GROUP BY p ORDER BY p"
$CLICKHOUSE_CLIENT -q "INSERT INTO th SELECT number, number % 3 FROM numbers(30, 30) SETTINGS $PR_SETTINGS"
check "SELECT count(), countIf(_path LIKE '%p=%') FROM th"

echo "-- lazily loaded table"
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_lazy ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${CLICKHOUSE_DATABASE}_lazy.ice ENGINE = IcebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
    DETACH DATABASE ${CLICKHOUSE_DATABASE}_lazy;
    ATTACH DATABASE ${CLICKHOUSE_DATABASE}_lazy;
"
$CLICKHOUSE_CLIENT -q "SELECT sum(id) FROM ${CLICKHOUSE_DATABASE}_lazy.ice SETTINGS $PR_SETTINGS"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${CLICKHOUSE_DATABASE}_lazy; ATTACH DATABASE ${CLICKHOUSE_DATABASE}_lazy;"
$CLICKHOUSE_CLIENT -q "EXPLAIN SELECT sum(id) FROM ${CLICKHOUSE_DATABASE}_lazy.ice SETTINGS $PR_SETTINGS" | grep -c 'ReadFromCluster'

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE ${CLICKHOUSE_DATABASE}_lazy;
    DROP VIEW v;
    DROP VIEW vn;
    DROP VIEW vc;
    DROP TABLE mt;
    DROP USER u_${CLICKHOUSE_DATABASE};
    DROP FUNCTION ${CLICKHOUSE_DATABASE}_f;
"
