-- Under `analyzer_alias_hides_table_name` the carriers of `t` in one enclosing scope are probed in the order of the
-- lookup there: lambda arguments, expression aliases, `ARRAY JOIN` outputs, columns, and only then table names, with
-- `prefer_column_name_to_alias` moving the expression aliases after the columns. The first carrier that binds `t`
-- decides: a scalar one stops the lookup of the qualifier of `t.*`, so a table `t` of the same scope does not hide the
-- name of `t AS c`, and the matcher expands the inner table, as with the setting disabled.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'constant alias and table';
WITH 1 AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM t;
WITH 1 AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM t SETTINGS analyzer_alias_hides_table_name = 0;

SELECT 'column and table';
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 5 AS t) AS t;

SELECT 'array join output and table';
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM t ARRAY JOIN [7] AS t;

SELECT 'scalar alias and tuple column';
WITH 1 AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT tuple(7) AS t) AS x SETTINGS prefer_column_name_to_alias = 0;
WITH 1 AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT tuple(7) AS t) AS x SETTINGS prefer_column_name_to_alias = 1;

SELECT 'tuple alias and scalar column';
WITH tuple(8) AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 5 AS t) AS x SETTINGS prefer_column_name_to_alias = 0;
WITH tuple(8) AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 5 AS t) AS x SETTINGS prefer_column_name_to_alias = 1;

DROP TABLE t;
