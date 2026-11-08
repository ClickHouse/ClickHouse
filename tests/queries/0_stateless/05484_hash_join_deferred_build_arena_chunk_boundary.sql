-- A deferred build projects the arena that its `String` keys take. The arena adds a chunk of 128 MiB for a key of
-- 128 MiB - 63 bytes, and the key fills it with its padding. For a key one byte shorter, one byte is left free. The
-- projection must count the key in the chunk it was added for. Otherwise it never ends for the first key. For the
-- second it projects petabytes, so a join under a spill threshold switches to `GraceHashJoin`.

SET join_algorithm = 'hash';
SET hash_join_layout = 'serial';
SET enable_hash_join_deferred_build = 1;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0; -- A runtime filter would copy the long key once more.
SET enable_parallel_replicas = 0;

-- A right key of 128 MiB - 64 bytes (8128 * 16513). Adding `number`, which is 0, makes it a full column, not a
-- constant. Under a 1 GB spill threshold the build defers and stays in memory.
SELECT count()
FROM (SELECT toString(number) AS k FROM numbers(1)) AS l
INNER JOIN (SELECT repeat(repeat('a', 8128 + number), 16513) AS k FROM numbers(1)) AS r ON l.k = r.k
SETTINGS max_bytes_before_external_join = 1000000000, log_comment = '05484_inner';

-- A right key of 128 MiB - 63 bytes (103165 * 1301). A `RIGHT SEMI` join defers without a spill threshold. It emits
-- no row here, so it does not copy the key as `RIGHT ALL` would.
SELECT count()
FROM (SELECT toString(number) AS k FROM numbers(1)) AS l
RIGHT SEMI JOIN (SELECT repeat(repeat('a', 103165 + number), 1301) AS k FROM numbers(1)) AS r ON l.k = r.k
SETTINGS max_bytes_before_external_join = 0, log_comment = '05484_right_semi';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinDeferredBuilds'], ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05484_inner', '05484_right_semi')
ORDER BY log_comment;
