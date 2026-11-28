-- A hash join with the parallel layout can build its tables before its last block, here when the keys built so far plus
-- the waiting rows reach `max_rows_in_join`. The blocks after that early build go into the tables it built, which grow
-- as they take the new keys. At build finish under `max_bytes_before_external_join`, `HashJoin::evaluateBuildFit` must
-- count that growth. The join below holds about 10 MB before building the last batch, which increases the key count
-- from about 9000 to 263144. The larger tables exceed the 20.5 MB fit budget.

-- The right side: 524288 rows in 64 blocks of 8192. The first half repeats the keys 0..999, the second half brings
-- 262144 new keys. The wait ends once 270000 rows wait, just after the first half; the final key count stays below the
-- limit.
CREATE VIEW right_rows AS
SELECT toUInt32(if(number < 262144, number % 1000, number)) AS k, toUInt32(number) AS v
FROM numbers(524288);

CREATE VIEW left_keys AS SELECT toUInt32(number * 3) AS k FROM numbers(300000);

SELECT 'early build, spilled', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 20500000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8192, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 270000, max_bytes_in_join = 0, join_overflow_mode = 'throw',
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = '05875_early_spilled';

SELECT 'serial', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0;

SYSTEM FLUSH LOGS query_log;

-- The early plan has 256 partitions, and the join switches to `GraceHashJoin`. `HashJoinBuildRounds` also counts a
-- later round, so it is not part of the check.
SELECT ProfileEvents['HashJoinBuildPartitions'] AS partitions,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched_to_grace
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05875_early_spilled';

DROP VIEW left_keys;
DROP VIEW right_rows;
