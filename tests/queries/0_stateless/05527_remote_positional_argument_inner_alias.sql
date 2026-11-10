-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS dn;
DROP TABLE IF EXISTS i;
DROP TABLE IF EXISTS `__table1`;
DROP VIEW IF EXISTS v;

CREATE TABLE t (number UInt64, k UInt64 DEFAULT number) ENGINE = MergeTree ORDER BY number;
INSERT INTO t (number) SELECT number FROM numbers(3);
CREATE TABLE `__table1` AS t;
INSERT INTO `__table1` SELECT number, number + 1 FROM t;
CREATE TABLE d1 AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE d2 AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), d1);
CREATE TABLE dn (number Nullable(UInt64)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE i (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v AS SELECT negate(number + 1 AS a) AS x FROM d1 ORDER BY 1;

SELECT 'ORDER BY';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1;
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY 1;
SELECT DISTINCT negate(number + 1 AS a) AS x FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1 DESC;
SELECT negate(number + 1 AS a) AS x, a AS y FROM remote('127.0.0.2', numbers(3)) WHERE a > 1 ORDER BY -2, -1;
WITH 5 AS n SELECT number + n AS x FROM remote('127.0.0.{2,3}', numbers(2)) ORDER BY 1;
SELECT intDiv(count() AS c, 2) AS x FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY number ORDER BY 1;
SELECT sum(number * 7) AS x FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1;
SELECT negate(length(hostName()) * 0 + number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT negate(number + 1 AS number) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 SETTINGS prefer_column_name_to_alias = 1;

SELECT 'repeated and nested aliases';
SELECT negate(number + 1 AS a) AS x, abs(number + 1 AS a) AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT negate(number + 1 AS a) AS x, negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT number + (1 AS one) AS a, negate(a) FROM remote('127.0.0.2', numbers(2)) ORDER BY 2;
SELECT number + (1 AS one) AS a, negate(a AS b) AS x, a + x * 3 AS y FROM remote('127.0.0.2', numbers(3)) ORDER BY 3, 2, 1;

SELECT 'GROUP BY';
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.{2,3}', numbers(5)) GROUP BY 1 ORDER BY x;
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.2', numbers(5)) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT intDiv(number + 1 AS a, 2) AS x, a % 2 AS y, sum(number) FROM remote('127.0.0.2', numbers(3)) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3;
SELECT negate(number % 2 + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(5)) GROUP BY GROUPING SETS ((1), ()) ORDER BY x, s;
SELECT negate(number % 2 + 1 AS a) AS x, count() FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY 1 WITH TOTALS ORDER BY 1;
SELECT negate(number + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS group_by_use_nulls = 1;
SELECT number % 2 AS y, toString(number % 2) AS x, max(x) AS m FROM remote('127.0.0.2', numbers(4)) GROUP BY GROUPING SETS ((1, 2), (1)) ORDER BY ALL;
SELECT number % 3 AS m, grouping(m) AS g FROM numbers(9) GROUP BY ROLLUP(m) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY ROLLUP(m) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY GROUPING SETS ((m), ()) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY GROUPING SETS ((1), ()) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY 1 WITH CUBE ORDER BY g, m;

SELECT 'arithmetic in aggregate functions';
SELECT number * 2 AS a, max(a) AS m FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2;
SELECT max((number * 2) AS a), min(a) AS m FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1, 2;
SELECT number % 2 AS k, sum(number * 7) AS s FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2;
SELECT number % 2 AS k, min(1 - number) AS s FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2;
WITH number / 2 AS h SELECT number % 2 AS k, avg(h) AS s, max(h) AS m FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2, 3;

SELECT 'LIMIT BY';
SELECT intDiv(number + 1 AS a, 2) AS x FROM remote('127.0.0.2', numbers(5)) ORDER BY x LIMIT 1 BY 1;
SELECT negate(number) AS a, negate(a) AS b FROM numbers(3) GROUP BY CUBE(1) ORDER BY 1 LIMIT 1 BY 2;
SELECT negate(number % 3) AS a, negate(a) AS b FROM numbers(6) GROUP BY a WITH TOTALS ORDER BY 1 LIMIT 1 BY 2;
SELECT negate(number % 3) AS a, negate(a) AS b FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 WITH TOTALS ORDER BY 1 LIMIT 1 BY 2;

SELECT 'subqueries';
SELECT x FROM (SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1 LIMIT 2) ORDER BY x;
WITH cte AS (SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1 LIMIT 2) SELECT x FROM cte ORDER BY x;
SELECT x FROM (SELECT negate(number + 1 AS a) AS x FROM d1 ORDER BY 1 LIMIT 1 UNION ALL SELECT negate(number + 2 AS a) AS x FROM d1 ORDER BY 1 LIMIT 1) ORDER BY x;
SELECT x FROM v ORDER BY x;
SELECT x FROM remote('127.0.0.2', view(SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1)) ORDER BY x;
SELECT number FROM remote('127.0.0.2', numbers(4)) WHERE -number IN (SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY 1 LIMIT 2) ORDER BY number;

SELECT 'predicate push down';
SELECT * FROM (SELECT negate(number + 1 AS k) AS x, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE y = 1;
SELECT * FROM (SELECT (number + 1 AS k, number + 2) AS x, negate(k) AS z, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1, 2) WHERE y = 1;
SELECT DISTINCT x, z FROM (SELECT negate(t.number + 1 AS number) AS x, t.k AS y, number AS z FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 3, 2, 1) WHERE z = 3;
SELECT x FROM (SELECT negate(number + 1 AS k) AS x, t.k AS y, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t GROUP BY 1, 2 ORDER BY 3, 1) WHERE y > 0 AND c = 2;

SELECT 'predicate push down: a filter could refer to the alias';
SELECT count() FROM (SELECT tuple(t.k + 1 AS k) AS x, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE y = 1;
SELECT count() FROM (SELECT multiIf(1, 5, number * 2 AS k) AS x, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE y = 1 SETTINGS serialize_query_plan = 0;
SELECT count() FROM (SELECT negate(number + 1) AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), `__table1`) AS t ORDER BY 1) WHERE y = 1;
SELECT count() FROM (SELECT t.k + 5 AS number, t.number AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE y = 1;

SELECT 'functions';
SELECT arraySum(l -> l + a, [number + 1 AS a]) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT arrayMap(l -> (l + 1 AS b) * b, [number + 1 AS a]) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT negate(number + toUInt16(1) AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT (number IN (SELECT number FROM numbers(1)) AS a) + 1 AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT (number + (1 AS a)) GLOBAL IN (SELECT 1) AS x, number FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT number + (1 GLOBAL IN (SELECT 1) AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT tuple(number + 1 AS a, 2).1 + 1 AS x, count() FROM remote('127.0.0.2', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS enable_named_columns_in_function_tuple = 1;

SELECT 'window functions';
SELECT negate(number + 1 AS a) AS x, sum(x) OVER (ORDER BY number) AS s FROM remote('127.0.0.2', numbers(3)) ORDER BY -1, -2;
SELECT row_number() OVER () + (1 AS one) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 DESC;

SELECT 'WITH FILL';
SELECT toInt32((number % 3 AS a) * 2) AS x FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 1 WITH FILL TO 6;
SELECT (number + 0 AS a) * 2 AS x, number AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 WITH FILL TO 4 INTERPOLATE (y AS a + 10);

SELECT 'ARRAY JOIN';
SELECT negate(n + 1 AS a) AS x FROM remote('127.0.0.{2,3}', view(SELECT [1, 2] AS arr)) ARRAY JOIN arr AS n ORDER BY 1;

SELECT 'JOIN USING';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) ORDER BY 1;
SELECT negate(number) AS x, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY 1 WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) AS t1 JOIN numbers(1, 5) AS t2 USING (number) ORDER BY 1;

SELECT 'FULL JOIN USING';
SELECT number, negate(number), count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY m;
SELECT number, negate(number), count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY m;
SELECT negate(number), count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY m;
SELECT negate(number), count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY m;
SELECT number, grouping(number) FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number WITH ROLLUP ORDER BY 1, 2;
SELECT number AS n, grouping(n) FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT * FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number ORDER BY 1 LIMIT 1 BY 1;
SELECT number, grouping(number) FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number WITH ROLLUP ORDER BY 1, 2;
SELECT number AS n, grouping(n) FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT * FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number ORDER BY 1 LIMIT 1 BY 1;
SELECT number, grouping(number) AS g FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY CUBE(number) ORDER BY 2, 1;
SELECT number, grouping(number) AS g FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY CUBE(number) ORDER BY 2, 1;

SELECT 'Distributed over Distributed';
SELECT negate(number + 1 AS a) AS x, count() FROM d2 GROUP BY 1 ORDER BY 1;

SELECT 'parallel replicas';
WITH number + 1 AS a SELECT negate(a) AS x, count() FROM t WHERE a > 1 GROUP BY 1 ORDER BY 1 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_local_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'serialized query plan';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 SETTINGS serialize_query_plan = 1;

SELECT 'WITH';
WITH negate(number + 1 AS a) AS x SELECT x, (SELECT max(a) FROM numbers(2)) AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2 SETTINGS enable_scopes_for_with_statement = 0;
WITH number + 1 AS a SELECT negate(a) AS x, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS enable_scopes_for_with_statement = 0;

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
DROP TABLE `__table1`;
DROP TABLE t;
