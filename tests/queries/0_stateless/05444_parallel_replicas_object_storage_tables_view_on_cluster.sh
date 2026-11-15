#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: ON CLUSTER is not allowed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PR_SETTINGS="enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_cluster_engines = 1,
    parallel_replicas_for_object_storage_tables = 1"

# A view created ON CLUSTER keeps `parallel_replicas_for_object_storage_tables` in its body, so it reads its table itself.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/f.parquet', format = 'Parquet')
    SELECT number AS a FROM numbers(600) SETTINGS s3_truncate_on_insert = 1;
    CREATE TABLE t (a UInt64) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/f.parquet', format = 'Parquet');
"
$CLICKHOUSE_CLIENT -q "
    CREATE VIEW v_off ON CLUSTER test_shard_localhost AS SELECT sum(a) FROM t SETTINGS parallel_replicas_for_object_storage_tables = 0;
    CREATE VIEW v_on ON CLUSTER test_shard_localhost AS SELECT sum(a) FROM t;
" > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT create_table_query LIKE '%parallel_replicas_for_object_storage_tables = 0%' FROM system.tables WHERE database = currentDatabase() AND name = 'v_off'"

for view in v_off v_on; do
    $CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_${view}" -q "SELECT * FROM ${view} SETTINGS $PR_SETTINGS"
done

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(query_id, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['Shards'] > 0 FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND query_id IN ('${CLICKHOUSE_DATABASE}_v_off', '${CLICKHOUSE_DATABASE}_v_on')
    ORDER BY query_id"

$CLICKHOUSE_CLIENT -q "DROP VIEW v_off; DROP VIEW v_on; DROP TABLE t"
