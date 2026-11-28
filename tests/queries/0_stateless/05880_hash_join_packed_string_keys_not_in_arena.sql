-- A hash join with one `String` key and `enable_packed_string_keys_in_hash_join` stores each key in a 16-byte cell: the
-- string itself, or its hash. Unlike a `String` map, it copies no characters to an arena. At build finish under
-- `max_bytes_before_external_join`, `HashJoin::evaluateBuildFit` must not count those characters.

-- The right side: 200000 unique keys of 40 characters, 8 MB. The join holds 14.1 MB of blocks, and builds to 21.95 MiB.
-- The build counts 27.5 MB at once, and fits the budget of 31 MB. Counting the characters as arena bytes, it would count
-- 35.4 MB, and switch to `GraceHashJoin`.
CREATE VIEW right_rows AS SELECT concat('key_', leftPad(toString(number), 36, '0')) AS k, toUInt32(number) AS v FROM numbers(200000);

CREATE VIEW left_keys AS SELECT concat('key_', leftPad(toString(number * 3), 36, '0')) AS k FROM numbers(100000);

SELECT 'packed keys, in memory', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 31000000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8000, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, enable_robin_hood_hash_for_joins = 0,
    enable_packed_string_keys_in_hash_join = 1, join_use_nulls = 0, collect_hash_table_stats_during_joins = 0,
    query_plan_join_swap_table = false, query_plan_optimize_join_order_randomize = 0,
    log_comment = '05880_packed_keys_in_memory';

SELECT 'serial', count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER JOIN right_rows AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched_to_grace
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05880_packed_keys_in_memory';

DROP VIEW left_keys;
DROP VIEW right_rows;
