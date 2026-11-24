-- An `ALL` hash join allocates the row-list nodes of its repeated keys in arenas, whose chunks double. At build finish
-- under `max_bytes_before_external_join`, `HashJoin::evaluateBuildFit` must count the chunks that the arenas allocate, not
-- only the bytes of the nodes: the build has no later check, and a build that is let through then holds more than the budget.

-- The right side: 120000 rows in blocks of 8000 with the keys 0..79999, so that the keys 0..39999 occur twice and the
-- others once. Below 131072 rows the build has one owner, so one arena takes all 40000 nodes: 2560000 bytes, in chunks of
-- 4 KiB to 2 MiB, 4190208 bytes. The join then holds about 9.35 MB. Counting the nodes as bytes, the build would take
-- 8.94 MB and fit the budget of 9.1 MB; counting the chunks, it does not fit, and the join switches to `GraceHashJoin`.
CREATE VIEW right_rows AS SELECT toUInt32(number % 80000) AS k, toUInt32(number) AS v FROM numbers(120000);

CREATE VIEW left_keys AS SELECT toUInt32(number * 3) AS k FROM numbers(100000);

SELECT 'arena chunks, spilled', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 9100000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8000, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, enable_robin_hood_hash_for_joins = 0,
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = '05879_arena_chunks_spilled';

SELECT 'serial', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched_to_grace
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05879_arena_chunks_spilled';

DROP VIEW left_keys;
DROP VIEW right_rows;
