-- Tags: shard

-- When a shard of a `Distributed` table moves the `HAVING` conjuncts on the grouping keys to `WHERE`,
-- it must not change the result of the conjuncts left in `HAVING` on the initiator.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_local_05452;
DROP TABLE IF EXISTS t_dist_05452;
CREATE TABLE t_local_05452 (k UInt64, s String, x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dist_05452 AS t_local_05452
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_local_05452');
INSERT INTO t_local_05452 SELECT number % 10, '2024-01-01 00:00:00', 1 FROM numbers(100);

-- `blockSize` depends on the block of groups it is evaluated on: a sibling conjunct moved to the shard `WHERE`
-- would leave only one group in the block. Every shard contributes 10 rows to the group.
SELECT '--- blockSize with a grouping-key conjunct';
SELECT k, count() FROM t_dist_05452 GROUP BY k HAVING blockSize() > 1 AND k = 0
    SETTINGS prefer_localhost_replica = 0, max_threads = 1, max_block_size = 100,
        group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, query_plan_enable_optimizations = 0;

-- `parseDateTime` without the time zone argument parses the string in the time zone of the server, which may differ
-- between the shards and the initiator, so the conjunct stays in `HAVING`. With the time zone argument it is moved.
SELECT '--- parseDateTime without a time zone';
SELECT s, sum(x) FROM t_dist_05452 GROUP BY s HAVING toUnixTimestamp(parseDateTime(s, '%F %T', 'UTC')) = 1704067200;
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05452 GROUP BY s HAVING toUnixTimestamp(parseDateTime(s, '%F %T')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05452 GROUP BY s HAVING toUnixTimestamp(parseDateTimeInJodaSyntax(s, 'yyyy-MM-dd HH:mm:ss')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT '--- parseDateTime with a time zone';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05452 GROUP BY s HAVING toUnixTimestamp(parseDateTime(s, '%F %T', 'UTC')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

DROP TABLE t_dist_05452;
DROP TABLE t_local_05452;
