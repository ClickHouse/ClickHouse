-- Under `analyzer_alias_hides_table_name` the alias of `t AS c` hides the name `t` from the qualifier of a
-- matcher when an enclosing query gives `t` a reading a matcher can expand: a `Tuple` expression alias,
-- column or `ARRAY JOIN` output. With `analyzer_compatibility_prefer_alias_over_subcolumn` the matcher
-- prefers a table expression of that name, so it must not find the hidden inner table and has to expand
-- the outer `Tuple`, the same as with the outer name renamed. A carrier of another type does not hide the
-- name, so the matcher still expands the inner table.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;
SET analyzer_compatibility_prefer_alias_over_subcolumn = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'expression alias';
WITH CAST(tuple(100), 'Tuple(val UInt64)') AS t SELECT (SELECT t.* FROM t AS c LIMIT 1);
WITH CAST(tuple(100), 'Tuple(val UInt64)') AS x SELECT (SELECT x.* FROM t AS c LIMIT 1);
WITH CAST(tuple(100), 'Tuple(val UInt64)') AS t SELECT (SELECT t.COLUMNS('val') FROM t AS c LIMIT 1);
WITH (100,) AS t SELECT (SELECT t.* FROM t AS c LIMIT 1);

SELECT 'column';
SELECT (SELECT t.* FROM t AS c LIMIT 1) FROM (SELECT CAST(tuple(100), 'Tuple(val UInt64)') AS t);

SELECT 'array join';
SELECT (SELECT t.* FROM t AS c LIMIT 1) FROM (SELECT 1) ARRAY JOIN [CAST(tuple(100), 'Tuple(val UInt64)')] AS t;

SELECT 'not expandable';
WITH 1 AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1);
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 5 AS t);
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 1) ARRAY JOIN [7] AS t;

SELECT 'setting disabled';
WITH CAST(tuple(100), 'Tuple(val UInt64)') AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) SETTINGS analyzer_alias_hides_table_name = 0;

DROP TABLE t;
