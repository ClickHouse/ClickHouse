-- Tags: no-fasttest, no-replicated-database, no-parallel-replicas
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`
-- Tag no-parallel-replicas: asserts on the plan and the merges of the initial query, which with parallel replicas reads the table with `ReadFromCluster`

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05859_aggregation_in_order_block_bytes_iceberg/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (k Int64, v Int64) ENGINE = IcebergLocal((SELECT path || 't/' FROM iceberg_path), 'Parquet') ORDER BY k;
INSERT INTO t SELECT number % 200, number FROM numbers(50000) SETTINGS max_block_size = 65536;
INSERT INTO t SELECT number % 200, number FROM numbers(50000, 50000) SETTINGS max_block_size = 65536;
CREATE TABLE t2 (k Int64, v Int64) ENGINE = IcebergLocal((SELECT path || 't2/' FROM iceberg_path), 'Parquet') ORDER BY k;
INSERT INTO t2 SELECT number % 10, number FROM numbers(50000) SETTINGS max_block_size = 65536;
INSERT INTO t2 SELECT number % 10, number FROM numbers(50000, 50000) SETTINGS max_block_size = 65536;

-- The partial results must be cut by size even while their growth stays inside the thread's untracked memory window.
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') FROM (EXPLAIN PIPELINE SELECT k, groupArray(v) FROM t GROUP BY k SETTINGS optimize_aggregation_in_order = 1, max_threads = 2, max_block_size = 65536, aggregation_in_order_max_block_bytes = 100000, max_untracked_memory = 4194304, memory_profiler_step = 4194304);
SELECT count(), sum(length(a)), sum(arraySum(a)) FROM (SELECT k, groupArray(v) AS a FROM t GROUP BY k SETTINGS optimize_aggregation_in_order = 1, max_threads = 2, max_block_size = 65536, aggregation_in_order_max_block_bytes = 100000, max_untracked_memory = 4194304, memory_profiler_step = 4194304);
SELECT k, groupArray(v) FROM t GROUP BY k FORMAT Null SETTINGS optimize_aggregation_in_order = 1, max_threads = 2, max_block_size = 65536, aggregation_in_order_max_block_bytes = 100000, max_untracked_memory = 4194304, memory_profiler_step = 4194304, log_comment = '05859_aggregation_in_order_block_bytes_iceberg_merge';

-- The input columns materialized for the call are not part of the partial results.
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') FROM (EXPLAIN PIPELINE SELECT k, max(toLowCardinality(concat(repeat('x', 40), toString(v % 3)))) FROM t2 GROUP BY k SETTINGS optimize_aggregation_in_order = 1, max_threads = 2, max_block_size = 65536, aggregation_in_order_max_block_bytes = 100000, max_untracked_memory = 4194304, memory_profiler_step = 4194304);
SELECT count(), countIf(m = concat(repeat('x', 40), '2')) FROM (SELECT k, max(toLowCardinality(concat(repeat('x', 40), toString(v % 3)))) AS m FROM t2 GROUP BY k SETTINGS optimize_aggregation_in_order = 1, max_threads = 2, max_block_size = 65536, aggregation_in_order_max_block_bytes = 100000, max_untracked_memory = 4194304, memory_profiler_step = 4194304);
SELECT k, max(toLowCardinality(concat(repeat('x', 40), toString(v % 3)))) FROM t2 GROUP BY k FORMAT Null SETTINGS optimize_aggregation_in_order = 1, max_threads = 2, max_block_size = 65536, aggregation_in_order_max_block_bytes = 100000, max_untracked_memory = 4194304, memory_profiler_step = 4194304, log_comment = '05859_aggregation_in_order_block_bytes_iceberg_input';

SYSTEM FLUSH LOGS text_log, query_log;
SET max_rows_to_read = 0;

SELECT count() >= 5 FROM system.text_log
WHERE event_date >= yesterday()
    AND query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05859_aggregation_in_order_block_bytes_iceberg_merge' AND type = 'QueryFinish')
    AND message_format_string = 'Merging partially aggregated blocks (bucket = {}).';

SELECT count() BETWEEN 1 AND 2 FROM system.text_log
WHERE event_date >= yesterday()
    AND query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05859_aggregation_in_order_block_bytes_iceberg_input' AND type = 'QueryFinish')
    AND message_format_string = 'Merging partially aggregated blocks (bucket = {}).';

DROP TABLE t;
DROP TABLE t2;
