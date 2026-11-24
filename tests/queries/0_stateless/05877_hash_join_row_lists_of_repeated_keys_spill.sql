-- An `ALL` hash join keeps the first row of a key in its cell, and allocates a 64-byte row-list node for the key once its
-- second row arrives. At build finish under `max_bytes_before_external_join`, `HashJoin::evaluateBuildFit` must count those
-- nodes also when most keys have one row: the average of rows per key is then below 2, but every repeated key takes a node.

-- The right side: 300000 rows in blocks of 8000 with the keys 0..199999, so that the keys 0..99999 occur twice and the
-- others once. The join holds 3042778 bytes, and the tables of 8 partitions take 8384512 bytes more. The row lists of the
-- 100000 repeated keys take 6400000 bytes; from the estimate of 195350 keys the build counts at most 7813888 bytes of
-- nodes for them, and twice that for the arena chunks that hold the nodes. With them the build does not fit the budget of
-- 15 MB, and the join switches to `GraceHashJoin`; without them it would fit.
CREATE VIEW right_rows AS SELECT toUInt32(number % 200000) AS k, toUInt32(number) AS v FROM numbers(300000);

CREATE VIEW left_keys AS SELECT toUInt32(number * 3) AS k FROM numbers(100000);

SELECT 'repeated keys, spilled', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 15000000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8000, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, enable_robin_hood_hash_for_joins = 0,
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = '05877_repeated_spilled';

SELECT 'serial', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched_to_grace
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05877_repeated_spilled';

DROP VIEW left_keys;
DROP VIEW right_rows;
