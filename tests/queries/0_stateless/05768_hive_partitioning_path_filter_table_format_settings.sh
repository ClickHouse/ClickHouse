#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on S3

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A filter on a Hive partition column of a `File`, `URL` or `S3` table converts the partition value of a path
# with the format settings of the table, as reading the column does.

PR_SETTINGS="enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_cluster_engines = 1,
    parallel_replicas_for_object_storage_tables = 1"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}/b=si/data.jsonl', JSONEachRow) SELECT 7 AS c0 SETTINGS engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/b=si/data.jsonl', format = JSONEachRow) SELECT 7 AS c0 SETTINGS s3_truncate_on_insert = 1;

    CREATE TABLE t_file (c0 Int32, b Bool) ENGINE = File(JSONEachRow, '${CLICKHOUSE_DATABASE}/b=si/data.jsonl') SETTINGS bool_true_representation = 'si';
    CREATE TABLE t_url (c0 Int32, b Bool) ENGINE = URL('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/b=si/data.jsonl', JSONEachRow) SETTINGS bool_true_representation = 'si';
    CREATE TABLE t_s3 (c0 Int32, b Bool) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/b=si/data.jsonl', format = JSONEachRow) SETTINGS bool_true_representation = 'si';
    CREATE TABLE t_s3_brace (c0 Int32, b Bool) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/b={si,false}/data.jsonl', format = JSONEachRow) SETTINGS bool_true_representation = 'si';
    CREATE TABLE t_s3_glob (c0 Int32, b Bool) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/b=*/data.jsonl', format = JSONEachRow) SETTINGS bool_true_representation = 'si';

    SELECT 'file', * FROM t_file WHERE b;
    SELECT 'file, GLOBAL IN', * FROM t_file WHERE b GLOBAL IN (SELECT true);
    SELECT 'url', * FROM t_url WHERE b;
    SELECT 's3', * FROM t_s3 WHERE b;
    SELECT 's3, GLOBAL IN', * FROM t_s3 WHERE b GLOBAL IN (SELECT true);
    SELECT 's3, parallel replicas', * FROM t_s3 WHERE b SETTINGS ${PR_SETTINGS};
    SELECT 's3 brace expansion', * FROM t_s3_brace WHERE b;
    SELECT 's3 brace expansion, GLOBAL IN', * FROM t_s3_brace WHERE b GLOBAL IN (SELECT true);
    SELECT 's3 glob', * FROM t_s3_glob WHERE b;
"
