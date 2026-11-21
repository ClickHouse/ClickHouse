-- Tags: shard, no-parallel
-- no-parallel: the databases have fixed names, because SQL cannot build a name from currentDatabase().

-- Tables whose database and table names joined by a dot give the same string are different tables
-- for scalar subqueries and IN sets of one query.

DROP DATABASE IF EXISTS db_05493;
DROP DATABASE IF EXISTS `db_05493.inner`;
DROP DATABASE IF EXISTS `db_05493.p`;
DROP DATABASE IF EXISTS `db_05493.p.q`;

CREATE DATABASE db_05493;
CREATE DATABASE `db_05493.inner`;
CREATE TABLE db_05493.`inner.t` (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05493.`inner.t` VALUES (1);
CREATE TABLE `db_05493.inner`.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO `db_05493.inner`.t VALUES (0), (2);

SELECT 'scalar', (SELECT max(x) FROM db_05493.`inner.t`), (SELECT max(x) FROM `db_05493.inner`.t);
SELECT 'scalar reversed', (SELECT max(x) FROM `db_05493.inner`.t), (SELECT max(x) FROM db_05493.`inner.t`);
SELECT 'array scalar', (SELECT arraySort(groupArray(x)) FROM db_05493.`inner.t`), (SELECT arraySort(groupArray(x)) FROM `db_05493.inner`.t);
SELECT 'in', 2 IN (SELECT x FROM db_05493.`inner.t`), 2 IN (SELECT x FROM `db_05493.inner`.t);
SELECT 'where in or in', count() FROM numbers(3) WHERE number IN (SELECT x FROM db_05493.`inner.t`) OR number IN (SELECT x FROM `db_05493.inner`.t);
SELECT 'group by', number IN (SELECT x FROM db_05493.`inner.t`) AS a, number IN (SELECT x FROM `db_05493.inner`.t) AS b, count()
FROM numbers(3) GROUP BY a, b ORDER BY a, b;
SELECT 'remote', countIf(number IN (SELECT x FROM db_05493.`inner.t`)), countIf(number IN (SELECT x FROM `db_05493.inner`.t))
FROM remote('127.0.0.{1,2}', numbers(3));

CREATE DATABASE `db_05493.p`;
CREATE DATABASE `db_05493.p.q`;
CREATE TABLE `db_05493.p`.`q.s` (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO `db_05493.p`.`q.s` VALUES (1);
CREATE TABLE `db_05493.p.q`.s (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO `db_05493.p.q`.s VALUES (2);
SELECT 'two dotted databases', (SELECT max(x) FROM `db_05493.p`.`q.s`), (SELECT max(x) FROM `db_05493.p.q`.s);

CREATE TABLE db_05493.tt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05493.tt VALUES (2);
CREATE TEMPORARY TABLE `db_05493.tt` (x UInt64) ENGINE = Memory;
INSERT INTO `db_05493.tt` VALUES (1);
SELECT 'temporary table scalar', (SELECT max(x) FROM `db_05493.tt`), (SELECT max(x) FROM db_05493.tt);
SELECT 'temporary table in', 2 IN (SELECT x FROM `db_05493.tt`), 2 IN (SELECT x FROM db_05493.tt);

DROP DATABASE db_05493;
DROP DATABASE `db_05493.inner`;
DROP DATABASE `db_05493.p`;
DROP DATABASE `db_05493.p.q`;
