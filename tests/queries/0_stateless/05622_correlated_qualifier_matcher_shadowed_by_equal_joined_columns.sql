-- Under `analyzer_alias_hides_table_name`, when both sides of a `JOIN` of an enclosing query bind `t` to equal columns,
-- the lookup there is not ambiguous: it reads that column, so an expression alias `t` does not take over, even with
-- `prefer_column_name_to_alias`. The columns are equal when they are the same column of two equal table expressions,
-- or, for an `INNER JOIN`, when the `ON` condition equates them. Then the `Tuple` column `t` hides the name of
-- `t AS c` from the qualifier of `t.*`, and `t.*` reads the outer column.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS u1;
DROP TABLE IF EXISTS u2;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

CREATE TABLE u1 (t Tuple(val UInt64)) ENGINE = Memory;
INSERT INTO u1 VALUES ((7));
CREATE TABLE u2 (t Tuple(val UInt64)) ENGINE = Memory;
INSERT INTO u2 VALUES ((7));

SELECT 'equal subqueries';
WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT tuple(7) AS t) AS a
CROSS JOIN (SELECT tuple(7) AS t) AS b
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT tuple(7) AS t) AS a
CROSS JOIN (SELECT tuple(7) AS t) AS b
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 0;

SELECT 'two references to one table';
WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM u1 AS a
CROSS JOIN u1 AS b
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'equated keys of an INNER JOIN';
WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM u1 AS a
INNER JOIN u2 AS b ON a.t = b.t
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'different subqueries stay ambiguous, the alias takes over';
WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT tuple(7) AS t) AS a
CROSS JOIN (SELECT tuple(8) AS t) AS b
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

DROP TABLE t;
DROP TABLE u1;
DROP TABLE u2;
