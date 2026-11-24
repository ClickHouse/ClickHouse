-- The first two blocks contain 128000 distinct keys. The last block adds 122000 keys with longer payloads.
-- The odd multiplier permutes the keys. At 256 partitions the fullest partition outgrows 1024 cells.
-- Each payload ends with its key. Keeping the longer final payloads puts bucket growth over the 38 MB limit.
CREATE VIEW right_rows AS
SELECT toUInt32(if(number < 262144, number % 128000, number - 134144)) AS k,
    if(number < 262144, toString(k), leftPad(toString(k), 192, '0')) AS payload
FROM numbers(384144);

CREATE VIEW left_keys AS SELECT toUInt32(number * 2) AS k FROM numbers(200000);

SELECT 'early build, spilled', count(), sum(r.k), sum(cityHash64(l.k, r.k))
FROM left_keys AS l INNER ANY JOIN right_rows AS r ON toUInt32(l.k * 2654435761) = toUInt32(r.k * 2654435761)
WHERE endsWith(r.payload, toString(l.k))
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 38000000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 131072, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 260000, max_bytes_in_join = 0, join_overflow_mode = 'throw',
    join_any_take_last_row = 1, enable_robin_hood_hash_for_joins = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = '05876_early_spilled';

SELECT 'serial', count(), sum(r.k), sum(cityHash64(l.k, r.k))
FROM left_keys AS l INNER ANY JOIN right_rows AS r ON toUInt32(l.k * 2654435761) = toUInt32(r.k * 2654435761)
WHERE endsWith(r.payload, toString(l.k))
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_any_take_last_row = 1,
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0;

SYSTEM FLUSH LOGS query_log;

-- The early plan has 256 partitions, and the join switches to `GraceHashJoin`.
SELECT ProfileEvents['HashJoinBuildPartitions'] AS partitions,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched_to_grace
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05876_early_spilled';

DROP VIEW left_keys;
DROP VIEW right_rows;
