-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS dn;
DROP TABLE IF EXISTS i;
DROP VIEW IF EXISTS v;

CREATE TABLE t (number UInt64, k UInt64 DEFAULT number) ENGINE = MergeTree ORDER BY number;
INSERT INTO t (number) SELECT number FROM numbers(3);
CREATE TABLE d1 AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE d2 AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), d1);
CREATE TABLE dn (number Nullable(UInt64)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE i (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v AS SELECT negate(number + 1 AS a) AS x FROM d1 ORDER BY 1;

SELECT 'subqueries';
SELECT x FROM (SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1 LIMIT 2) ORDER BY x;
WITH cte AS (SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1 LIMIT 2) SELECT x FROM cte ORDER BY x;
SELECT x FROM (SELECT negate(number + 1 AS a) AS x FROM d1 ORDER BY 1 LIMIT 1 UNION ALL SELECT negate(number + 2 AS a) AS x FROM d1 ORDER BY 1 LIMIT 1) ORDER BY x;
SELECT x FROM v ORDER BY x;
SELECT x FROM remote('127.0.0.2', view(SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1)) ORDER BY x;
SELECT number FROM remote('127.0.0.2', numbers(4)) WHERE -number IN (SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY 1 LIMIT 2) ORDER BY number;

SELECT 'Distributed over Distributed';
SELECT negate(number + 1 AS a) AS x, count() FROM d2 GROUP BY 1 ORDER BY 1;

SELECT 'parallel replicas';
WITH number + 1 AS a SELECT negate(a) AS x, count() FROM t WHERE a > 1 GROUP BY 1 ORDER BY 1 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_local_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'the remote server folds the expression and its aliases';
SELECT multiIf(1, 5, number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT and(number > 0 AS a, hostName() = 'no such host') AS x, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY 1;
SELECT concat(hostName() AS a, toString(number)) = concat(hostName(), '1') AS x, number FROM remote('127.0.0.2', numbers(3)) ORDER BY 1, 2;
SELECT isNull(number + 1 AS a) AS x, number FROM dn ORDER BY 1, 2;
SELECT if(isNull(any(number)) AS a, 1, 2) AS x, number FROM remote('127.0.0.2', numbers(3)) GROUP BY number ORDER BY 1, 2;
SELECT arrayJoin([1, 2] AS a) AS x, number FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT negate(COLUMNS('number')) AS x FROM remote('127.0.0.2', numbers(3)) AS t1 LEFT JOIN numbers(2) AS t2 USING (number) ORDER BY 1;
SELECT number GLOBAL IN (SELECT 1) AS x, number FROM remote('127.0.0.2', numbers(3)) ORDER BY 1, 2;

SELECT 'INSERT SELECT';
INSERT INTO i SELECT intDiv(number + 1 AS a, 2) AS x, count() FROM d1 GROUP BY 1 SETTINGS prefer_localhost_replica = 1;
INSERT INTO i SELECT intDiv(number + 1 AS a, 2) AS x, count() + 10 FROM d1 GROUP BY 1 SETTINGS prefer_localhost_replica = 0;
SELECT * FROM i ORDER BY ALL;

DROP VIEW v;
DROP TABLE i;
DROP TABLE dn;
DROP TABLE d2;
DROP TABLE d1;
DROP TABLE t;
