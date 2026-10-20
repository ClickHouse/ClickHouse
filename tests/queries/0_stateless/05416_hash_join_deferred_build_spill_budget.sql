-- A deferred build under the spill threshold of `SpillingHashJoin`. Under the threshold it inserts its keys and keeps
-- the join in memory; over it, it hands its stored blocks to `GraceHashJoin` and reserves nothing. A
-- `LowCardinality(Nullable(String))` key does the same in both layouts.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET grace_hash_join_initial_buckets = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_r (k UInt64, s LowCardinality(Nullable(String)), v UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t_l SELECT number * 3, if(number % 5 = 0, NULL, toString(number % 3000)) FROM numbers(100000);
INSERT INTO t_r SELECT number % 200000, if(number % 7 = 0, NULL, toString(number % 3000)), number FROM numbers(400000);

SELECT 'inner', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0;
SELECT 'inner', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 1073741824, log_comment = '05416_under';
SELECT 'inner', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 1048576, log_comment = '05416_over';

SELECT 'low_cardinality_nullable', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0;
SELECT 'low_cardinality_nullable', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 1073741824, log_comment = '05416_under';
SELECT 'low_cardinality_nullable', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 1048576, log_comment = '05416_over';
SELECT 'low_cardinality_nullable', count(), sum(cityHash64(l.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 1073741824, parallel_hash_join_threshold = 18446744073709551615,
    log_comment = '05416_under';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    count(),
    countIf(ProfileEvents['HashJoinDeferredBuilds'] = 1),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0),
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] = 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select' AND log_comment IN ('05416_under', '05416_over')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_l;
DROP TABLE t_r;
