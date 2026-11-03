-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- The files are in a directory of their own for each database: a concurrent run of the test rewrites the files it
-- writes, which invalidates the entry of the schema cache for them.

INSERT INTO FUNCTION s3('http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 1 SETTINGS s3_truncate_on_insert = 1;

-- Hive partition columns are inferred from the same path the reader materializes.
SELECT 'hive';
SELECT c1, ts, toTypeName(ts), k FROM url('http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv') SETTINGS use_hive_partitioning = 1, enable_url_encoding = 0;
SELECT c1, ts, toTypeName(ts), k FROM url('http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv') SETTINGS use_hive_partitioning = 1, enable_url_encoding = 1;
SELECT c1, ts, toTypeName(ts), k FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv') SETTINGS use_hive_partitioning = 1, enable_url_encoding = 1;

SET use_hive_partitioning = 1, enable_url_encoding = 1;
DROP TABLE IF EXISTS t_05442_url;
CREATE TABLE t_05442_url ENGINE = URL('http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv');
SELECT c1, ts, toTypeName(ts), k FROM t_05442_url;
DROP TABLE t_05442_url;

-- The schema inferred for a URL with spaces is found in the schema cache by the next query.
SELECT 'schema cache';
DESCRIBE url('http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv') SETTINGS use_hive_partitioning = 0, schema_inference_use_cache_for_url = 1, log_comment = '05442_first' FORMAT Null;
DESCRIBE url('http://localhost:11111/test/' || currentDatabase() || '/05442 dir/ts=2026-10-22 12:00:00/k=a b/f.csv') SETTINGS use_hive_partitioning = 0, schema_inference_use_cache_for_url = 1, log_comment = '05442_second' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['SchemaInferenceCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05442_second' AND event_date >= yesterday();
