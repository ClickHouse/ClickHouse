-- Tags: shard

-- A cast of a container converts each nested number as a Unix timestamp to a `Date` or `Date32` in the time zone
-- of the server, which may differ between the shards and the initiator, so a shard of a `Distributed` table must keep
-- such a conjunct in `HAVING` instead of moving it to `WHERE`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_local_05458;
DROP TABLE IF EXISTS t_dist_05458;
CREATE TABLE t_local_05458 (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dist_05458 AS t_local_05458
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_local_05458');
INSERT INTO t_local_05458 SELECT 1704067200 + number % 10, 1 FROM numbers(100);

SELECT '--- number cast to a date inside a container';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT k, sum(x) FROM t_dist_05458 GROUP BY k HAVING CAST([k] AS Array(Date)) = [toDate('2024-01-01')]
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT k, sum(x) FROM t_dist_05458 GROUP BY k HAVING CAST(tuple(k) AS Tuple(Date32)) = tuple(toDate32('2024-01-01'))
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT k, sum(x) FROM t_dist_05458 GROUP BY k HAVING CAST(map('a', k) AS Map(String, Date))['a'] = toDate('2024-01-01')
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);
SELECT countIf(explain ILIKE '%Filter (WHERE)%') AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT k, sum(x) FROM t_dist_05458 GROUP BY k HAVING CAST([k] AS Array(Nullable(Date))) = [toDate('2024-01-01')]
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

SELECT '--- container cast without a date';
SELECT countIf(explain ILIKE '%Filter (WHERE)%') > 0 AS shard_filters
FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT k, sum(x) FROM t_dist_05458 GROUP BY k HAVING CAST([k] AS Array(String)) = ['1704067200']
    SETTINGS prefer_localhost_replica = 0, query_plan_enable_optimizations = 0
);

SELECT k, sum(x) FROM t_dist_05458 GROUP BY k HAVING CAST([k] AS Array(Date)) = [toDate('2024-01-01')] ORDER BY k
SETTINGS session_timezone = 'UTC';

DROP TABLE t_dist_05458;
DROP TABLE t_local_05458;
