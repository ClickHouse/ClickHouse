-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- Reading in order more files than `read_in_order_two_level_merge_threshold` merges them in one group per stream.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05857_iceberg_read_in_order_merge_per_stream/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (k Int64) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet') ORDER BY k;
-- Eight sorted files, each covering the whole key range.
INSERT INTO t VALUES (0), (8), (16), (24);
INSERT INTO t VALUES (1), (9), (17), (25);
INSERT INTO t VALUES (2), (10), (18), (26);
INSERT INTO t VALUES (3), (11), (19), (27);
INSERT INTO t VALUES (4), (12), (20), (28);
INSERT INTO t VALUES (5), (13), (21), (29);
INSERT INTO t VALUES (6), (14), (22), (30);
INSERT INTO t VALUES (7), (15), (23), (31);

SELECT count(), uniqExact(_path) FROM t
SETTINGS max_threads = 4, read_in_order_two_level_merge_threshold = 4, optimize_read_in_order = 1, enable_parallel_replicas = 0;

-- 4 streams: 4 groups of 2 files and the final merge; no sort.
SELECT countIf(explain ILIKE '%MergingSortedTransform%'), countIf(explain ILIKE '%PartialSortingTransform%')
FROM (EXPLAIN PIPELINE SELECT k FROM t ORDER BY k)
SETTINGS max_threads = 4, read_in_order_two_level_merge_threshold = 4, optimize_read_in_order = 1, enable_parallel_replicas = 0;

SELECT groupArray(k) FROM (SELECT k FROM t ORDER BY k)
SETTINGS max_threads = 4, read_in_order_two_level_merge_threshold = 4, optimize_read_in_order = 1, enable_parallel_replicas = 0;

SELECT groupArray(k) FROM (SELECT DISTINCT k FROM t ORDER BY k)
SETTINGS max_threads = 4, read_in_order_two_level_merge_threshold = 4, optimize_read_in_order = 1, enable_parallel_replicas = 0;

DROP TABLE t;
