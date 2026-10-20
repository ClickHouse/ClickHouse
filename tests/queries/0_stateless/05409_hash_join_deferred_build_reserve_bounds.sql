-- The deferred build reserves for the distinct keys it inserts. Rows with a NULL key or failing the right-side ON
-- condition do not count, and duplicates count once.
--
-- N = 100000 distinct keys, each 4 times, plus 10 * N rows with a NULL key and 10 * N rows that fail `r.keep = 1`: 24 * N
-- rows in all. With a sketch error of up to 6%, the serial layout reserves at most 1.06 * N. The two-level layout adds
-- a margin of four standard deviations to each of its 256 buckets, so at most 1.27 * N.
--
-- With a hot key that has 90% of the rows, no bucket reserves more than four even shares.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET enable_hash_join_deferred_build = 1;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_hot;

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_build (k Nullable(UInt64), keep UInt8) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_hot (k UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_probe SELECT number FROM numbers(1000);
INSERT INTO t_build SELECT intDiv(number, 4), 1 FROM numbers(400000);
INSERT INTO t_build SELECT NULL, 1 FROM numbers(1000000);
INSERT INTO t_build SELECT 100000 + number, 0 FROM numbers(1000000);
-- 900000 rows of key 0, then 100000 distinct keys.
INSERT INTO t_hot SELECT if(number < 900000, 0, number) FROM numbers(1000000);

SELECT 'masked', count() FROM t_probe l LEFT JOIN t_build r ON l.k = r.k AND r.keep = 1
SETTINGS parallel_hash_join_threshold = 0, log_comment = '05409_two_level';
SELECT 'masked', count() FROM t_probe l LEFT JOIN t_build r ON l.k = r.k AND r.keep = 1
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '05409_serial';
SELECT 'hot', count() FROM t_probe l INNER JOIN t_hot r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 0, log_comment = '05409_hot';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['HashJoinDeferredBuilds'],
    ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables']
        BETWEEN 94000 AND multiIf(log_comment = '05409_serial', 106000, log_comment = '05409_two_level', 127000, 250000),
    ProfileEvents['HashJoinDeferredBuildBucketsGrown']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment IN ('05409_two_level', '05409_serial', '05409_hot')
ORDER BY log_comment;

DROP TABLE t_probe;
DROP TABLE t_build;
DROP TABLE t_hot;
