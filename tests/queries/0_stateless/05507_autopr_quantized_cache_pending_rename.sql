-- Tags: zookeeper

-- The columns of a reloaded part lose the custom serialization of the `Quantized` codec, which the reader then takes from
-- the requested column when the part names the column differently, as with a pending rename. The granules served from
-- the columns cache are not measured then, as for any `Quantized` column, since reading the vectors skips the codes and
-- the codebook that the column lists. The table never gets a leader, so the rename stays pending after the reload.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_quantized_pending_rename;
CREATE TABLE t_quantized_pending_rename (key UInt64, vec Array(Float32) CODEC(Quantized('product', 8, 4, 2)))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{shard}/t_quantized_pending_rename', '{replica}') ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replicated_can_become_leader = 0;
INSERT INTO t_quantized_pending_rename SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(2e5);
-- Waiting for the rename would wait for its mutation, which no replica assigns: wait for the metadata only.
ALTER TABLE t_quantized_pending_rename RENAME COLUMN vec TO vec_renamed SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_quantized_pending_rename;
DETACH TABLE t_quantized_pending_rename;
ATTACH TABLE t_quantized_pending_rename;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

SELECT vec_renamed FROM t_quantized_pending_rename FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05507_read_1';
SELECT vec_renamed FROM t_quantized_pending_rename FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05507_read_2';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_quantized_pending_rename';

SYSTEM FLUSH LOGS query_log;

-- A read is measured if the cache served nothing. Which granules it serves is not fixed: other tests can evict the
-- entries before the second read.
SELECT log_comment, if(ProfileEvents['ColumnsCacheHits'] = 0,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] = 0) AS measured_only_without_cache_hits
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment LIKE '05507\\_read\\_%' AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_quantized_pending_rename;
