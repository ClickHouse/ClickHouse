-- A hash join with the parallel layout keeps its right blocks until the build, which scatters their keys to the partitions
-- and inserts them. Two kinds of build do that in more than one round. Each later round inserts into the tables of the
-- first, next to row references into the blocks of the earlier rounds:
-- 1. A `Grouped` fit at build finish: under `max_bytes_before_external_join` the copy of all keys does not fit, but the
--    tables and the copy of one block do. The join builds in rounds over ranges of blocks instead of spilling.
-- 2. An early build: the wait ends before the last block, and the blocks after it go into the tables built so far, with
--    no new partition plan.
-- Each result is compared with the same join built with the serial layout and no spill threshold. Every parallel query
-- pins the layout, 4 build streams, blocks of 8192 rows that are not squashed, and the settings that decide the fit and
-- the end of the wait.

-- The right side: 524288 rows in 64 blocks of 8192. The first half repeats the keys 0..999. The second half has the keys
-- 0..1999: it adds rows to the keys of the first half and brings 1000 new keys. `v` tells the rows apart. `w` depends on
-- the key only, so an `ANY` join returns the same `w` whichever row of a key it keeps.
CREATE VIEW right_rows AS
SELECT toUInt32(if(number < 262144, number % 1000, number % 2000)) AS k, toUInt32(number) AS v, toUInt32(k * 7 + 3) AS w
FROM numbers(524288);

-- The left side: the keys 500..2999. The right keys 0..499 and the left keys 2000..2999 have no match.
CREATE VIEW left_keys AS SELECT toUInt32(number + 500) AS k FROM numbers(2500);

-- 1. INNER ALL with a `Grouped` fit. At build finish the join holds 6,336,448 bytes and projects 11,588,384 bytes
-- for tables and arenas. Scattering all rows takes 6,291,456 bytes (12 bytes per row). With four partitions,
-- the at-once peak is 6,336,448 + 11,588,384 + 6,291,456 / 4 = 19,497,696 bytes. Building in rounds needs only
-- one block of scatter space, for 6,336,448 + 11,588,384 + 8,192 * 12 = 18,023,136 bytes. Only this plan fits the 18,700,000-byte budget.
SELECT 'inner all, grouped', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 18700000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8192, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'inner_all_grouped';

SELECT 'inner all, serial', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'inner_all_serial';

-- 2. FULL ALL with a `Grouped` fit: the same blocks and budget. The right rows without a match are read from the used
-- flags after the rounds.
SELECT 'full all, grouped', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l FULL ALL JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 18700000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8192, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'full_all_grouped';

SELECT 'full all, serial', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l FULL ALL JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_use_nulls = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'full_all_serial';

-- 3. INNER ALL with an early build, compared with the serial result of 1. The limit `max_rows_in_join` counts keys, and
-- every waiting row may be a new key. So the wait ends once the keys built so far plus the waiting rows reach the limit:
-- here every 8 blocks. The tables never hold more than 2000 keys, so the limit itself is never reached.
SELECT 'inner all, early', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.v), sum(cityHash64(l.k, r.k, r.v))
FROM left_keys AS l INNER ALL JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8192, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 65536, max_bytes_in_join = 0, join_overflow_mode = 'throw',
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'inner_all_early';

-- 4. LEFT ANY with an early build. An `ANY` join keeps one row per key, so once new rows stop bringing new keys it ends
-- its wait and builds, and can drop the blocks that no key points at. Here that happens once 100000 rows wait, in the
-- first half, and again in later batches. The later rounds drop the blocks of the repeated keys, and must keep the blocks
-- that the earlier rounds point at.
SELECT 'left any, early', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.w), sum(cityHash64(l.k, r.k, r.w))
FROM left_keys AS l LEFT ANY JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8192, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_any_take_last_row = 0,
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'left_any_early';

SELECT 'left any, serial', multiIf(l.k = 0, 'right only', l.k >= 2000, 'left only', l.k < 1000, 'repeated key', 'new key') AS rows_of,
    count(), sum(r.w), sum(cityHash64(l.k, r.k, r.w))
FROM left_keys AS l LEFT ANY JOIN right_rows AS r ON l.k = r.k
GROUP BY rows_of ORDER BY rows_of
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, max_rows_in_join = 0, max_bytes_in_join = 0, join_any_take_last_row = 0,
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, log_comment = 'left_any_serial';

SYSTEM FLUSH LOGS query_log;

-- `HashJoinBuildPartitions` counts the partitions of the one plan of a build, made in its first round: 4 for a plan at
-- build finish (4 build streams, about 2000 keys), 256 for the plan of an early build. No join switched to
-- `GraceHashJoin` or wrote a file, and each built its table in memory with the layout it asked for.
SELECT log_comment, ProfileEvents['HashJoinBuildRounds'] > 1 AS several_rounds, ProfileEvents['HashJoinBuildPartitions'] AS partitions,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched_to_grace, ProfileEvents['ExternalJoinWritePart'] AS files_written,
    ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('inner_all_grouped', 'inner_all_serial', 'full_all_grouped', 'full_all_serial', 'inner_all_early',
        'left_any_early', 'left_any_serial')
ORDER BY log_comment;

DROP VIEW left_keys;
DROP VIEW right_rows;
