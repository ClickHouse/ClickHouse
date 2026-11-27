-- Tags: no-fasttest
-- no-fasttest: 1M-row joins, about 18 of them, and one of 4M rows.
-- Random settings limits: max_block_size=(65409, 65409); grace_hash_join_initial_buckets=(1, 1); query_plan_optimize_join_order_randomize=(0, 0); query_plan_read_in_order_through_join=(0, 0); enable_join_runtime_filters=(0, 0)

-- A hash join that fits in `max_bytes_before_external_join` stays in memory, and one that does not switches to
-- `GraceHashJoin` before it goes over. The right side (1M unique keys, one payload column) takes about 72 MiB in
-- memory and at most about 90 MiB while it is built, so it fits in 110 MB and not in 30 MB. Every query returns
-- `count()` and `sum()` of a right column: a right block lost or inserted twice while the join switches changes them.

SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 'false';
SET enable_robin_hood_hash_for_joins = 0;

SELECT 'serial fits', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'serial', max_threads = 4, max_bytes_before_external_join = 110000000, log_comment = 'spill_at_threshold_serial_fits';

SELECT 'serial spills', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'serial', max_threads = 4, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_serial_spills';

SELECT 'parallel fits', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 110000000, log_comment = 'spill_at_threshold_parallel_fits';

SELECT 'parallel spills', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_parallel_spills';

-- 16 build threads, and a grace bucket that splits while the blocks of the in-memory join are handed over to it.
SELECT 'parallel spills, 16 threads', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 16, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_parallel16_spills';

-- One build thread: the buckets of the parallel layout sit behind one slot.
SELECT 'parallel fits, 1 thread', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 1, max_bytes_before_external_join = 110000000, log_comment = 'spill_at_threshold_parallel1_fits';

SELECT 'parallel spills, 1 thread', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 1, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_parallel1_spills';

-- `grace_hash` splits a bucket only when it would not fit either: one bucket here, so nothing is written to disk.
SELECT 'grace_hash fits', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS join_algorithm = 'grace_hash', max_threads = 4, max_bytes_before_external_join = 110000000, log_comment = 'spill_at_threshold_grace_fits';

SELECT 'grace_hash spills', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS join_algorithm = 'grace_hash', max_threads = 4, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_grace_spills';

-- Four times the rows: the bucket splits several times, and each split leaves it a table sized for the rows it keeps.
SELECT 'grace_hash splits several times', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(4000000)) AS l
INNER JOIN (SELECT number AS k, number * 3 AS v FROM numbers(4000000)) AS r ON l.k = r.k
SETTINGS join_algorithm = 'grace_hash', max_threads = 2, max_bytes_before_external_join = 60000000, log_comment = 'spill_at_threshold_grace_splits';

-- `RIGHT` and `FULL` joins keep the right rows that match nothing, and a condition on both sides keeps a used flag per row.
SELECT 'right fits', count(), sum(r.v), sum(l.k)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
RIGHT JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 110000000, log_comment = 'spill_at_threshold_right_fits';

SELECT 'right spills', count(), sum(r.v), sum(l.k)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
RIGHT JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_right_spills';

SELECT 'full, right condition, spills', count(), sum(r.v), sum(l.k)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
FULL JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k AND r.v % 2 = 0
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_full_right_cond_spills';

SELECT 'full, mixed condition, fits', count(), sum(r.v), sum(l.k)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
FULL JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k AND l.k < r.v
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 110000000, log_comment = 'spill_at_threshold_full_mixed_cond_fits';

SELECT 'full, mixed condition, spills', count(), sum(r.v), sum(l.k)
FROM (SELECT number AS k FROM numbers(1000000)) AS l
FULL JOIN (SELECT number AS k, number * 3 AS v FROM numbers(1000000)) AS r ON l.k = r.k AND l.k < r.v
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 30000000, log_comment = 'spill_at_threshold_full_mixed_cond_spills';

-- Ten right rows per key, so the join keeps a row list per key.
SELECT 'duplicate keys spill', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(100000)) AS l
INNER JOIN (SELECT number % 100000 AS k, number AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 10000000, log_comment = 'spill_at_threshold_duplicate_keys_spills';

-- One key in every right row: a split cannot separate it, so the bucket goes over the threshold instead of
-- splitting until it runs out of buckets. `v` is 0 in every row, so `sum` does not depend on the row that
-- `ANY` and `SEMI` keep.
SELECT 'one key, any left', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1)) AS l
ANY LEFT JOIN (SELECT number % 1 AS k, intDiv(number, 10000) AS v FROM numbers(10000)) AS r USING (k)
SETTINGS hash_join_layout = 'serial', max_threads = 1, max_block_size = 512, max_bytes_before_external_join = 45000;

SELECT 'one key, left semi', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1)) AS l
LEFT SEMI JOIN (SELECT number % 1 AS k, intDiv(number, 10000) AS v FROM numbers(10000)) AS r USING (k)
SETTINGS hash_join_layout = 'serial', max_threads = 1, max_block_size = 512, max_bytes_before_external_join = 45000;

SELECT 'one key, grace_hash', count(), sum(r.v)
FROM (SELECT number AS k FROM numbers(1)) AS l
INNER JOIN (SELECT number % 1 AS k, number AS v FROM numbers(1000000)) AS r USING (k)
SETTINGS join_algorithm = 'grace_hash', max_threads = 1, max_bytes_before_external_join = 3000000;

-- NULL keys after one key, and one key after NULL keys: the kept NULL rows have a hash of their own, so a split takes
-- them off the key.
SELECT 'one key, then NULL keys', count(), sum(r.v), countIf(l.k = 0)
FROM (SELECT toNullable(number) AS k FROM numbers(2)) AS l
RIGHT JOIN (SELECT if(number < 65409, toNullable(toUInt64(0)), NULL) AS k, number AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS join_algorithm = 'grace_hash', max_threads = 1, max_bytes_before_external_join = 3000000, log_comment = 'spill_at_threshold_one_key_then_nulls';

SELECT 'NULL keys, then one key', count(), sum(r.v), countIf(l.k = 0)
FROM (SELECT toNullable(number) AS k FROM numbers(2)) AS l
RIGHT JOIN (SELECT if(number < 65409, NULL, toNullable(toUInt64(0))) AS k, number AS v FROM numbers(1000000)) AS r ON l.k = r.k
SETTINGS join_algorithm = 'grace_hash', max_threads = 1, max_bytes_before_external_join = 3000000, log_comment = 'spill_at_threshold_nulls_then_one_key';

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, 'spill_at_threshold_', '') AS query,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched,
    ProfileEvents['ExternalJoinUncompressedBytes'] > 0 AS wrote_to_disk
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'spill\_at\_threshold\_%'
ORDER BY event_time_microseconds;

-- A join that spills switches before it holds its whole right side (about 72 MiB), so its query peak stays below that.
SELECT replaceOne(log_comment, 'spill_at_threshold_', '') AS query, memory_usage < 76000000 AS switched_early
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'spill_at_threshold_serial_spills';

-- The same for `grace_hash`, which splits a bucket instead of inserting a block that would take it over the threshold.
SELECT replaceOne(log_comment, 'spill_at_threshold_', '') AS query, memory_usage < 80000000 AS split_early
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'spill_at_threshold_grace_spills';

-- After several splits the peak stays near that of one bucket: a table twice the size of the one it needs takes it past 108 MB.
SELECT replaceOne(log_comment, 'spill_at_threshold_', '') AS query, memory_usage < 108000000 AS sized_after_split
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'spill_at_threshold_grace_splits';
