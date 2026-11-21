-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- A `key = value` table function argument computed from the current database must have the same value on every server
-- the query is sent to, where the current database is `default`.

SET use_hive_partitioning = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05416/p0=a/data.jsonl', format = JSONEachRow)
    SETTINGS s3_truncate_on_insert = 1 SELECT 1 AS rid;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05416/p0=x/data.jsonl', format = JSONEachRow)
    SETTINGS s3_truncate_on_insert = 1 SELECT 2 AS rid;

-- Parallel replicas over a cluster engine: the replicas infer the hive column from their own path.
SELECT rid, p0 FROM s3(s3_conn, filename = currentDatabase() || '/05416/p0=*/data.jsonl', format = JSONEachRow) ORDER BY rid
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_mode = 'read_tasks',
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_cluster_engines = 1;

-- A cluster table function.
SELECT rid, p0 FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn,
    filename = currentDatabase() || '/05416/p0=*/data.jsonl', format = JSONEachRow) ORDER BY rid;

-- The remote server reads the table function itself.
SELECT rid, p0 FROM remote('127.0.0.2', s3(s3_conn,
    filename = currentDatabase() || '/05416/p0=*/data.jsonl', format = JSONEachRow)) ORDER BY rid;

-- The table function in an IN subquery evaluated by two shards.
SELECT countIf(number IN (SELECT rid FROM s3(s3_conn,
    filename = currentDatabase() || '/05416/p0=*/data.jsonl', format = JSONEachRow))) FROM remote('127.0.0.{1,2}', numbers(4));

-- Numeric values in an IN subquery evaluated by two shards. A serialized query plan sends a table function with its
-- non-literal values hidden, so the shards would read '[HIDDEN]'.
SELECT countIf(number IN (SELECT rid FROM s3(s3_conn, filename = currentDatabase() || '/05416/p0=*/data.jsonl',
    format = JSONEachRow, max_single_read_retries = toInt16(4), max_connections = toInt64(100)))) FROM remote('127.0.0.{1,2}', numbers(4))
SETTINGS serialize_query_plan = 0;
