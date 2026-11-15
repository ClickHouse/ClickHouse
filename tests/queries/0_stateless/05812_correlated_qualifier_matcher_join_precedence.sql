-- Under `analyzer_alias_hides_table_name`, the probe of an enclosing query follows the join resolution of the lookup:
-- columns equated by `isNotDistinctFrom` (`<=>`) in the `ON` condition of an `INNER JOIN` are not ambiguous, like
-- columns equated by `equals`, and an `ARRAY JOIN` output below a `JOIN` competes with the columns of the other side
-- of that `JOIN` instead of preceding the whole join tree.

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

SELECT 'keys equated by isNotDistinctFrom';
WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM u1 AS a
INNER JOIN u2 AS b ON isNotDistinctFrom(a.t, b.t)
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM u1 AS a
INNER JOIN u2 AS b ON a.t <=> b.t
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'control: renamed';
WITH 1 AS x
SELECT a.x, b.x, (SELECT x.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT t AS x FROM u1) AS a
INNER JOIN (SELECT t AS x FROM u2) AS b ON a.x <=> b.x
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0;

SELECT 'ARRAY JOIN output below a JOIN is ambiguous with the other side, the alias takes over';
WITH (8,) AS t
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT [5] AS arr) AS a ARRAY JOIN arr AS t
INNER JOIN (SELECT tuple(7) AS t) AS b ON true
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'control: renamed';
WITH (8,) AS x
SELECT (SELECT x.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT [5] AS arr) AS a ARRAY JOIN arr AS x
INNER JOIN (SELECT tuple(7) AS x) AS b ON true
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0;

DROP TABLE t;
DROP TABLE u1;
DROP TABLE u2;
