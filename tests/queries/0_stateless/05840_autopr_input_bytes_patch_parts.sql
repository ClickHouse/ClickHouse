-- The input bytes of the statistics include what the readers of patch parts load: a replica reading the same granules
-- applies the same patches. Here an unmerged lightweight update replaces empty strings with payloads that hardly compress,
-- so the patch part is nearly all of what the query reads.

DROP TABLE IF EXISTS t_autopr_patch_bytes;

CREATE TABLE t_autopr_patch_bytes (key UInt64, payload String) ENGINE = MergeTree ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_autopr_patch_bytes;
INSERT INTO t_autopr_patch_bytes SELECT number, '' FROM numbers(100000);

SET allow_experimental_lightweight_update = 1;
-- 64 hex characters that hardly compress; `sipHash128` because builds without SSL have no `SHA256`.
UPDATE t_autopr_patch_bytes SET payload = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;

-- Mode 2 only collects the statistics, which `RuntimeDataflowStatisticsInputBytes` reports.
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_query_condition_cache = 0;

SELECT key % 10 AS k, max(payload) FROM t_autopr_patch_bytes GROUP BY k FORMAT Null
SETTINGS log_comment = '05840_autopr_input_bytes_patch_parts';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The payloads take at least 3.2 MB compressed (100000 rows of 32 random bytes); the base part, with empty strings, a
-- small fraction of that.
SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 3000000 AS patch_counted
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05840_autopr_input_bytes_patch_parts')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_patch_bytes;
