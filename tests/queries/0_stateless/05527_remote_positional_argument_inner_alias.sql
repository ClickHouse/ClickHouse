-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857
-- A position in a query is replaced with a copy of the expression before the query is sent, so the aliases inside are repeated there.
-- The queries failed with `MULTIPLE_EXPRESSIONS_FOR_ALIAS`, except for the ones that returned a wrong answer, and the controls.

SET enable_positional_arguments = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS d3;
DROP TABLE IF EXISTS dn;
DROP TABLE IF EXISTS di;
DROP TABLE IF EXISTS i;
DROP TABLE IF EXISTS b;
DROP VIEW IF EXISTS m;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS `__table1`;
DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS vc;
DROP DICTIONARY IF EXISTS dict;

CREATE TABLE t (number UInt64, k UInt64 DEFAULT number) ENGINE = MergeTree ORDER BY number;
INSERT INTO t (number) SELECT number FROM numbers(3);
CREATE TABLE `__table1` AS t;
INSERT INTO `__table1` SELECT number, number + 1 FROM t;
CREATE TABLE d1 AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE d2 AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), d1);
CREATE TABLE d3 AS t ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t);
CREATE TABLE i (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE di AS i ENGINE = Distributed(test_shard_localhost, currentDatabase(), i);
CREATE TABLE s (number UInt64) ENGINE = MergeTree ORDER BY number;
CREATE TABLE b AS s ENGINE = Buffer(currentDatabase(), s, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
CREATE MATERIALIZED VIEW m ENGINE = MergeTree ORDER BY number AS SELECT number, x FROM s JOIN (SELECT number, negate(number + 1 AS a) AS x FROM d1 ORDER BY 2 LIMIT 3) AS r USING (number);
CREATE TABLE dn (number Nullable(UInt64)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE VIEW vc (x Int64) AS SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY negate(number + 1) AS x;
CREATE DICTIONARY dict (id UInt64, v Int64) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST '127.0.0.2' PORT tcpPort() USER 'default' QUERY 'SELECT toUInt64(1) AS id, toInt64((SELECT k FROM (SELECT *, negate(number + 1) AS x FROM (SELECT number, number AS k FROM numbers(3)) AS t ORDER BY negate(number + 1 AS k) AS x LIMIT 1) AS m CROSS JOIN (SELECT 100 AS k) AS n)) AS v')) LAYOUT(FLAT()) LIFETIME(0);
CREATE VIEW v AS SELECT negate(number + 1 AS a) AS x FROM d1 ORDER BY 1;

SELECT 'local queries are not changed';
SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY negate(number + 1) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT negate(number + 1 AS a) AS x, negate(number + 1 AS b) AS x FROM numbers(2) ORDER BY x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM numbers(5) GROUP BY intDiv(number + 1, 2) AS x ORDER BY x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT DISTINCT intDiv(number + 1 AS a, 2) AS x FROM numbers(5) ORDER BY intDiv(number + 1 AS b, 2) AS x DESC LIMIT 1 BY intDiv(number + 1, 2) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT negate(t.number + 1) AS x FROM t AS t WHERE t.k = 1 ORDER BY negate(t.number + 1 AS k) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT number + 1 AS a, negate(number + 1) AS x FROM t HAVING a > 1 ORDER BY negate(number + 1 AS b) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
-- The inner alias changes the name of a column, adds a correlated column, or hides the argument of a lambda.
SELECT k FROM (SELECT *, negate(number + 1) AS x FROM t ORDER BY negate(number + 1 AS k) AS x LIMIT 1) AS m CROSS JOIN (SELECT 100 AS k) AS n; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT t.number + 0 AS y, negate(t.k + 1) AS b FROM t AS t WHERE EXISTS (SELECT 1 FROM numbers(3) AS n WHERE n.number = y) ORDER BY negate(t.k + 1 AS t) AS b; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT t.k + 1 AS y, arrayMap(t -> y, [CAST(tuple(10), 'Tuple(k Int64)')]) AS z, negate(t.number + 1) AS b FROM t AS t ORDER BY negate(t.number + 1 AS t) AS b; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'ORDER BY';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1;
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY 1;
SELECT DISTINCT negate(number + 1 AS a) AS x FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1 DESC;
SELECT negate(number + 1 AS a) AS x, a AS y FROM remote('127.0.0.2', numbers(3)) WHERE a > 1 ORDER BY -2, -1;
WITH 5 AS n SELECT number + n AS x FROM remote('127.0.0.{2,3}', numbers(2)) ORDER BY 1;
SELECT intDiv(count() AS c, 2) AS x FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY number ORDER BY 1;

SELECT 'repeated and nested aliases';
SELECT negate(number + 1 AS a) AS x, abs(number + 1 AS a) AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT negate(number + 1 AS a) AS x, negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT number + (1 AS one) AS a, negate(a) FROM remote('127.0.0.2', numbers(2)) ORDER BY 2;
SELECT number + (1 AS one) AS a, negate(a AS b) AS x, a + x * 3 AS y FROM remote('127.0.0.2', numbers(3)) ORDER BY 3, 2, 1;

SELECT 'mixed with a function that only the remote server folds';
SELECT negate(length(hostName()) * 0 + number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;

SELECT 'shadowed column';
SELECT negate(number + 1 AS number) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 SETTINGS prefer_column_name_to_alias = 1;

SELECT 'GROUP BY';
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.{2,3}', numbers(5)) GROUP BY 1 ORDER BY x;
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.2', numbers(5)) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT intDiv(number + 1 AS a, 2) AS x, a % 2 AS y, sum(number) FROM remote('127.0.0.2', numbers(3)) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3;
SELECT negate(number % 2 + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(5)) GROUP BY GROUPING SETS ((1), ()) ORDER BY x, s;
SELECT negate(number % 2 + 1 AS a) AS x, count() FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY 1 WITH TOTALS ORDER BY 1;
SELECT negate(number + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS group_by_use_nulls = 1;

SELECT 'LIMIT BY';
SELECT intDiv(number + 1 AS a, 2) AS x FROM remote('127.0.0.2', numbers(5)) ORDER BY x LIMIT 1 BY 1;

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

SELECT 'predicate push down: the answer was wrong';
SELECT count() FROM (SELECT tuple(t.k + 1 AS k) AS x, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE y = 1;
SELECT count() FROM (SELECT multiIf(1, 5, number * 2 AS k) AS x, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE y = 1 SETTINGS serialize_query_plan = 0;
SELECT count() FROM (SELECT negate(number + 1) AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), `__table1`) AS t ORDER BY 1) WHERE y = 1;
SELECT count() FROM (SELECT arrayMap(k -> k + t.k, [number + 1]) AS x FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY 1) WHERE x = [3];
SELECT count() FROM (SELECT t.number + 1 AS x, t.number AS y FROM remote('127.0.0.{2,3}', numbers(9 AS number)) AS t ORDER BY 1) WHERE y = 3;
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), `__table1`) AS t ORDER BY 1, 2) WHERE y = 3;
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

SELECT 'Distributed over Distributed';
SELECT negate(number + 1 AS a) AS x, count() FROM d2 GROUP BY 1 ORDER BY 1;

SELECT 'parallel replicas';
WITH number + 1 AS a SELECT negate(a) AS x, count() FROM t WHERE a > 1 GROUP BY 1 ORDER BY 1 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_local_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'serialized query plan';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 SETTINGS serialize_query_plan = 1;

SELECT 'INSERT SELECT';
INSERT INTO i SELECT intDiv(number + 1 AS a, 2) AS x, count() FROM d1 GROUP BY 1 SETTINGS prefer_localhost_replica = 1;
INSERT INTO i SELECT intDiv(number + 1 AS a, 2) AS x, count() + 10 FROM d1 GROUP BY 1 SETTINGS prefer_localhost_replica = 0;
SELECT * FROM i ORDER BY ALL;
-- The buffer is flushed without a query.
INSERT INTO b VALUES (1);
OPTIMIZE TABLE b;
SELECT * FROM m ORDER BY ALL;

SELECT 'controls: the query is sent as it is written';
INSERT INTO di SELECT number, (SELECT k FROM (SELECT *, negate(number + 1) AS x FROM (SELECT number, number AS k FROM numbers(3)) AS t ORDER BY negate(number + 1 AS k) AS x LIMIT 1) AS m CROSS JOIN (SELECT 100 AS k) AS n) FROM d1 SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, distributed_foreground_insert = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), i) SELECT number, (SELECT k FROM (SELECT *, negate(number + 1) AS x FROM (SELECT number, number AS k FROM numbers(3)) AS t ORDER BY negate(number + 1 AS k) AS x LIMIT 1) AS m CROSS JOIN (SELECT 100 AS k) AS n) FROM remote('127.0.0.2', currentDatabase(), t) SETTINGS parallel_distributed_insert_select = 1, prefer_localhost_replica = 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT dictGet(currentDatabase() || '.dict', 'v', toUInt64(1)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'controls: the query of a view is resolved as it is written';
SELECT * FROM remote('127.0.0.2', view(SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY negate(number + 1) AS x)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM remote('127.0.0.2', currentDatabase(), vc); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'WITH';
WITH negate(number + 1 AS a) AS x SELECT x, (SELECT max(a) FROM numbers(2)) AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2 SETTINGS enable_scopes_for_with_statement = 0;
WITH number + 1 AS a SELECT negate(a) AS x, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS enable_scopes_for_with_statement = 0;

SELECT 'controls';
SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY 1;
SELECT (number % 2) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT number, negate(number), count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number), count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, grouping(number) FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number WITH ROLLUP ORDER BY 1, 2;
SELECT number, grouping(number) FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number WITH ROLLUP ORDER BY 1, 2;
SELECT negate(number), count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number), count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number + 1 AS a) AS x, count() FROM remote('127.0.0.2', numbers(2)) GROUP BY 1 WITH ROLLUP ORDER BY 1 NULLS FIRST SETTINGS group_by_use_nulls = 1;
SELECT number + 2 AS x, number + toUInt8(2) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 2 DESC SETTINGS serialize_query_plan = 0;
SELECT number + (1 AS l) AS y, arrayMap(l -> toNullable(l) + (number + 1 AS y), [1]), count() FROM remote('127.0.0.2', numbers(2)) GROUP BY 1, 2 WITH ROLLUP ORDER BY 2, 1 SETTINGS group_by_use_nulls = 1;
-- The query in `view` is sent as it is written, and it can use its aliases by name.
SELECT * FROM remote('127.0.0.2', view(WITH 2 AS two SELECT plus(two, 3) AS x, b FROM numbers(1) ORDER BY plus(two, (3 AS b)) AS x));

SELECT 'controls: the remote server folds the expression and its aliases';
SELECT multiIf(1, 5, number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT and(number > 0 AS a, hostName() = 'no such host') AS x, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY 1;
SELECT concat(hostName() AS a, toString(number)) = concat(hostName(), '1') AS x, number FROM remote('127.0.0.2', numbers(3)) ORDER BY 1, 2;
SELECT isNull(number + 1 AS a) AS x, number FROM dn ORDER BY 1, 2;
SELECT if(isNull(any(number)) AS a, 1, 2) AS x, number FROM remote('127.0.0.2', numbers(3)) GROUP BY number ORDER BY 1, 2;
SELECT arrayJoin([1, 2] AS a) AS x, number FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT negate(COLUMNS('number')) AS x FROM remote('127.0.0.2', numbers(3)) AS t1 LEFT JOIN numbers(2) AS t2 USING (number) ORDER BY 1;
SELECT number GLOBAL IN (SELECT 1) AS x, number FROM remote('127.0.0.2', numbers(3)) ORDER BY 1, 2;

SELECT 'controls: the inner alias is the name of a tuple element';
SELECT toJSONString(tuple(number + 1 AS v)) AS x, number FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2 SETTINGS enable_named_columns_in_function_tuple = 1, serialize_query_plan = 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'controls: a name refers to the inner alias';
SELECT number FROM remote('127.0.0.2', numbers(5)) WHERE (number, 0) IN (SELECT t2.k AS a, (number * 2 AS k) * 0 AS b FROM (SELECT number, number AS k FROM numbers(3)) AS t1 JOIN (SELECT number AS k FROM numbers(5)) AS t2 USING (k) WHERE t1.k < 10 ORDER BY 2 LIMIT 9 UNION ALL SELECT 100, 0 SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1) ORDER BY number SETTINGS serialize_query_plan = 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT t.k AS y, sum(negate(number + 1 AS k)) AS s FROM d3 AS t GROUP BY 1 ORDER BY 2 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k', prefer_localhost_replica = 0, serialize_query_plan = 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'controls: the remote server folds the expression, and its inner alias is used by name';
SELECT and(hostName() = 'no such host', (number * 2 AS k) > 0) AS x, t2.k FROM remote('127.0.0.2', view(SELECT number, number AS k FROM numbers(3))) AS t1 JOIN (SELECT number AS k FROM numbers(5)) AS t2 USING (k) ORDER BY 1, 2 SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1, serialize_query_plan = 0;
SELECT number FROM remote('127.0.0.2', numbers(5)) WHERE (number, 0) IN (SELECT t2.k, and(hostName() = 'no such host', (number * 2 AS k) > 0) AS x FROM (SELECT number, number AS k FROM numbers(3)) AS t1 JOIN (SELECT number AS k FROM numbers(5)) AS t2 USING (k) ORDER BY 2 LIMIT 9 SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1) ORDER BY number SETTINGS serialize_query_plan = 0;
SELECT number FROM remote('127.0.0.2', numbers(5)) WHERE (number, 0) IN (SELECT t2.k, and(hostName() = 'no such host', (number * 2 AS k) > 0) AS x FROM (SELECT number, number AS k FROM numbers(3)) AS t1 JOIN (SELECT number AS k FROM numbers(5)) AS t2 USING (k) ORDER BY 2 LIMIT 9 UNION ALL SELECT 100, 0 SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1) ORDER BY number SETTINGS serialize_query_plan = 0;
SELECT and(hostName() = 'no such host', (number * 2 AS k) > 0) AS x, number AS y FROM remote('127.0.0.2', numbers(3)) ORDER BY 1 WITH FILL TO 3, 2 INTERPOLATE (k AS 1) SETTINGS serialize_query_plan = 0, enable_parallel_replicas = 0;
SELECT and(hostName() = 'no such host', (number * 2 AS k) > 0) AS x, count() FROM d3 GROUP BY 1 ORDER BY 1 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k', prefer_localhost_replica = 0, serialize_query_plan = 0;

DROP TABLE dn;
DROP TABLE di;
DROP TABLE i;
DROP TABLE b;
DROP VIEW m;
DROP TABLE s;
DROP TABLE d3;
DROP VIEW v;
DROP VIEW vc;
DROP DICTIONARY dict;
DROP TABLE `__table1`;
DROP TABLE d2;
DROP TABLE d1;
DROP TABLE t;
