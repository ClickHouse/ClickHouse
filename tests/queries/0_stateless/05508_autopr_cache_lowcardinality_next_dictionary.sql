-- Each granule of an insert is written in one call and fills a `LowCardinality` dictionary, which is written at the end of
-- the call: the next mark points to the dictionary of the next granule, which a read of the granule does not load. The
-- granules served from the columns cache must not be priced with it.

DROP TABLE IF EXISTS t_lc_next_dictionary;
CREATE TABLE t_lc_next_dictionary (key UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
    ratio_of_defaults_for_sparse_serialization = 1;
-- A merge may write a granule in several calls.
SYSTEM STOP MERGES t_lc_next_dictionary;
INSERT INTO t_lc_next_dictionary SELECT number, hex(sipHash128(number)) || hex(sipHash128(number + 1)) FROM numbers(65536);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

SELECT s FROM t_lc_next_dictionary WHERE key < 8192 FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05508_read_1';
SELECT s FROM t_lc_next_dictionary WHERE key < 8192 FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05508_read_2';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Other tests can evict the entries before the second read, which then reads the part again.
SELECT min(input_bytes) > 0 AND max(input_bytes) <= min(input_bytes) * 1.25 AS cache_priced_as_part
FROM
(
    SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment LIKE '05508\\_read\\_%' AND type = 'QueryFinish'
);

DROP TABLE t_lc_next_dictionary;
