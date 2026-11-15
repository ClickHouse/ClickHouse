-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS `__table1`;
CREATE TABLE t (number UInt64, k UInt64 DEFAULT number) ENGINE = MergeTree ORDER BY number;
INSERT INTO t (number) SELECT number FROM numbers(3);
CREATE TABLE `__table1` AS t;
INSERT INTO `__table1` SELECT number, number + 1 FROM t;

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

DROP TABLE `__table1`;
DROP TABLE t;
