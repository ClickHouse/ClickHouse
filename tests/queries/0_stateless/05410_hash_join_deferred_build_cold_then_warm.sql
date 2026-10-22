-- A join whose size is not known defers its build. Its statistics then give the size to the next run of the same join,
-- which does not defer: the two-level layout reserves from the statistics, the serial layout does not reserve.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 1;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET enable_hash_join_deferred_build = 1;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r_two_level;
DROP TABLE IF EXISTS t_r_serial;

CREATE TABLE t_l (k UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_r_two_level (k UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_r_serial (k UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t_l SELECT number FROM numbers(1000);
INSERT INTO t_r_two_level SELECT number % 50000 FROM numbers(100000);
INSERT INTO t_r_serial SELECT number % 50000 FROM numbers(100000);

SELECT count() FROM t_l l INNER JOIN t_r_two_level r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 0, log_comment = '05410_two_level_cold';
SELECT count() FROM t_l l INNER JOIN t_r_two_level r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 0, log_comment = '05410_two_level_warm';
SELECT count() FROM t_l l INNER JOIN t_r_serial r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '05410_serial_cold';
SELECT count() FROM t_l l INNER JOIN t_r_serial r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '05410_serial_warm';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['HashJoinDeferredBuilds'],
    ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0,
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0,
    ProfileEvents['HashJoinBuiltWithSerialLayout']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment LIKE '05410_%'
ORDER BY event_time_microseconds;

DROP TABLE t_l;
DROP TABLE t_r_two_level;
DROP TABLE t_r_serial;
