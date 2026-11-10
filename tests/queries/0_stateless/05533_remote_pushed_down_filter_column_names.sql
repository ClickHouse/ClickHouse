-- Tags: shard
-- A filter that is pushed down into a subquery over remote tables must not refer to an alias or a lambda argument of the subquery.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP VIEW IF EXISTS v;

CREATE TABLE t (number UInt64, k UInt64, `n.a` Array(UInt64), `c.d` UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO t SELECT number, number + 1, [number], number * 10 FROM numbers(3);
CREATE TABLE d1 AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE d2 AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
CREATE VIEW v AS SELECT number AS k, k AS y FROM d2 ORDER BY number SETTINGS prefer_column_name_to_alias = 1;

SET prefer_localhost_replica = 0;

SELECT 'alias';
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM d2 AS t ORDER BY k, y) WHERE y = 3;
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY k, y) WHERE y = 3;
SELECT k, y FROM (SELECT t.number AS k, t.k AS y FROM d1 AS t ORDER BY 1) WHERE k = 2;
SELECT k, number FROM (SELECT t.number AS k, t.k AS number FROM d2 AS t GROUP BY 1, 2 ORDER BY 1) WHERE k = number - 1 AND number > 1 ORDER BY k;
SELECT k FROM (SELECT negate(t.number) AS k, t.k AS y, t.number AS z FROM d2 AS t ORDER BY z) WHERE y >= 2 AND z < 2;
SELECT k FROM (SELECT DISTINCT t.number AS k, t.k AS y FROM d2 AS t ORDER BY k) WHERE y = 3;
SELECT s, y FROM (SELECT sum(t.number) AS s, t.k AS y, max(t.number) AS k FROM d2 AS t GROUP BY t.k ORDER BY s) WHERE y = 3;
SELECT k FROM v WHERE y = 3;

SELECT 'lambda argument';
SELECT count() FROM (SELECT arrayMap(k -> k + t.k, [number]) AS x FROM d2 AS t ORDER BY x) WHERE x = [3];

SELECT 'table functions';
SELECT x FROM (SELECT t.number + 1 AS x, t.number AS y FROM remote('127.0.0.{2,3}', numbers(9 AS number)) AS t ORDER BY x) WHERE y = 3;
SELECT number FROM (SELECT t.number * 2 AS number, t.number AS y FROM remote('127.0.0.2', numbers(9)) AS t ORDER BY y) WHERE y = 3;

SELECT 'subqueries';
SELECT k FROM (SELECT q.number AS k, q.k AS y FROM (SELECT number, k FROM d2) AS q ORDER BY k) WHERE y = 3;
SELECT k FROM (SELECT number AS k, y FROM (SELECT t.number AS number, t.k AS y FROM d2 AS t ORDER BY number) ORDER BY k) WHERE y = 3;
SELECT x FROM (SELECT t.number AS k, t.k AS x FROM d2 AS t ORDER BY k) WHERE x IN (SELECT number + 2 FROM numbers(2)) ORDER BY x;
SELECT x FROM (SELECT t.number AS k, t.k AS x FROM d2 AS t ORDER BY k) WHERE x GLOBAL IN (SELECT number + 2 FROM numbers(2)) ORDER BY x;

SELECT 'names with dots';
SELECT k, a FROM (SELECT t.number AS k, t.n.a AS a, t.k AS `n.a` FROM d2 AS t ORDER BY k) WHERE a = [1];
SELECT k, d FROM (SELECT t.number AS k, t.`c.d` AS d, t.k AS `c.d` FROM d2 AS t ORDER BY k) WHERE d = 10;

SELECT 'parallel replicas';
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM t ORDER BY k, y) WHERE y = 3 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_local_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

DROP VIEW v;
DROP TABLE d2;
DROP TABLE d1;
DROP TABLE t;
