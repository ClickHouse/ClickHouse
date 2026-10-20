-- A runtime filter of the left side is built from the right side, and a deferred build fills its maps in the post-build
-- phase. The filter must still let through every left row that has a match. The `UInt32` keys span a small range, so
-- after the inserts the map turns into a flat array, which then serves as the filter.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k UInt64, k32 UInt32, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_r (k UInt64, k32 UInt32, v UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t_l SELECT number, number % 2000, number * 3 FROM numbers(200000);
INSERT INTO t_r SELECT number * 7, number % 1000, number FROM numbers(20000);

SELECT 'u64', count(), sum(cityHash64(l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0;
SELECT 'u64', count(), sum(cityHash64(l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05411_on';
SELECT 'u64', count(), sum(cityHash64(l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05411_on';

SELECT 'u32_range', count(), sum(cityHash64(l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k32 = r.k32
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 18446744073709551615;
SELECT 'u32_range', count(), sum(cityHash64(l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k32 = r.k32
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05411_on';

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(ProfileEvents['HashJoinDeferredBuilds'] = 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select' AND log_comment = '05411_on';

DROP TABLE t_l;
DROP TABLE t_r;
