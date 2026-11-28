-- Tags: shard

-- `toWeek`, `toYearWeek` and `toDayOfWeek` parse a string argument in the time zone of the server, unless the time zone
-- argument is given, and the time zone may differ between the shards and the initiator, so a shard of a `Distributed`
-- table must keep such a conjunct in `HAVING` instead of moving it to `WHERE`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_local_05459;
DROP TABLE IF EXISTS t_dist_05459;
CREATE TABLE t_local_05459 (s String, x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dist_05459 AS t_local_05459
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_local_05459');
INSERT INTO t_local_05459 SELECT '2024-01-07T23:30:00Z', 1 FROM numbers(100);

SELECT '--- implicit time zone';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toWeek(s, 3) = 1
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toYearWeek(s, 3) = 202401
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toDayOfWeek(s) = 7
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

SELECT '--- explicit time zone';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toWeek(s, 3, 'UTC') = 1
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toYearWeek(s, 3, 'UTC') = 202401
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toDayOfWeek(s, 0, 'UTC') = 7
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toWeek(s, 3) = 1
SETTINGS session_timezone = 'UTC', prefer_localhost_replica = 0;
SELECT s, sum(x) FROM t_dist_05459 GROUP BY s HAVING toWeek(s, 3, 'UTC') = 1
SETTINGS prefer_localhost_replica = 0;

DROP TABLE t_dist_05459;
DROP TABLE t_local_05459;
