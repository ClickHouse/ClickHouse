-- Tags: shard

-- An empty time zone argument of the `parseDateTime` family means the time zone of the server,
-- which may differ between the shards and the initiator, so a shard of a `Distributed` table must keep such a conjunct
-- in `HAVING` instead of moving it to `WHERE`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_local_05453;
DROP TABLE IF EXISTS t_dist_05453;
CREATE TABLE t_local_05453 (k UInt64, s String, x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dist_05453 AS t_local_05453
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_local_05453');
INSERT INTO t_local_05453 SELECT 1704067200 + number % 10, '2024-01-01 00:00:00', 1 FROM numbers(100);

SELECT '--- empty time zone';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05453 GROUP BY s HAVING toUnixTimestamp(parseDateTime(s, '%F %T', '')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05453 GROUP BY s HAVING toUnixTimestamp(parseDateTime64(s, '%F %T', '')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05453 GROUP BY s HAVING toUnixTimestamp(parseDateTimeInJodaSyntax(s, 'yyyy-MM-dd HH:mm:ss', '')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

SELECT '--- explicit time zone';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT s, sum(x) FROM t_dist_05453 GROUP BY s HAVING toUnixTimestamp(parseDateTime(s, '%F %T', 'UTC')) = 1704067200
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT k, sum(x) FROM t_dist_05453 GROUP BY k HAVING fromUnixTimestamp(k, '%F', 'UTC') = '2024-01-01'
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

SELECT s, sum(x) FROM t_dist_05453 GROUP BY s HAVING toUnixTimestamp(parseDateTime(s, '%F %T', 'UTC')) = 1704067200;

DROP TABLE t_dist_05453;
DROP TABLE t_local_05453;
