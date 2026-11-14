-- Tags: shard, no-parallel-replicas
-- The `HAVING` -> `WHERE` rewrite on `Distributed` must skip the arguments of table functions that are deliberately
-- left unresolved, like the query of `view` and the arguments of a parameterized view.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_src_05457;
DROP TABLE IF EXISTS t_dist_05457;
DROP VIEW IF EXISTS pv_05457;

CREATE TABLE t_src_05457 (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dist_05457 AS t_src_05457
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_src_05457');
INSERT INTO t_src_05457 SELECT number, number FROM numbers(10);

CREATE VIEW pv_05457 AS SELECT number + 1 AS n FROM numbers(10) WHERE number < {cnt:UInt64};

SELECT '--- view';
SELECT k, sum(x) FROM t_dist_05457 GROUP BY k
    HAVING k IN (SELECT n FROM view(SELECT number + 1 AS n FROM numbers(5)))
    ORDER BY k;
SELECT k, sum(x) FROM t_dist_05457 GROUP BY k
    HAVING k IN (SELECT n FROM view(SELECT number + 1 AS n FROM numbers(5)))
    ORDER BY k
    SETTINGS query_plan_enable_optimizations = 0;

SELECT '--- parameterized view';
SELECT k, sum(x) FROM t_dist_05457 GROUP BY k
    HAVING k IN (SELECT n FROM pv_05457(cnt = 5))
    ORDER BY k;
SELECT k, sum(x) FROM t_dist_05457 GROUP BY k
    HAVING k IN (SELECT n FROM pv_05457(cnt = 5))
    ORDER BY k
    SETTINGS query_plan_enable_optimizations = 0;

DROP VIEW pv_05457;
DROP TABLE t_dist_05457;
DROP TABLE t_src_05457;
