-- Under `analyzer_alias_hides_table_name` a table `t` of an enclosing query hides the name of `t AS c` from the
-- qualifier of a matcher only when the lookup of `t` reaches it. A nearer carrier of `t` that the matcher cannot
-- expand - a scalar lambda argument, constant alias, column or `ARRAY JOIN` output - stops the lookup of the qualifier
-- there, so the farther table gives it no reading, and the matcher expands the inner table, the same as with the outer
-- table renamed or the setting disabled.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'lambda argument';
WITH t -> (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) AS f SELECT f(1) FROM t;
WITH t -> (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) AS f SELECT f(1) FROM t AS o;
WITH t -> (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) AS f SELECT f(1) FROM t SETTINGS analyzer_alias_hides_table_name = 0;
WITH t -> (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) AS f SELECT f(1) FROM t SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
WITH t -> (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) AS f SELECT f(1) FROM t SETTINGS enable_global_with_statement = 0;

SELECT 'constant alias';
SELECT (WITH 1 AS t SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)) FROM t;

SELECT 'column';
SELECT (SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 5 AS t)) FROM t;

SELECT 'array join';
SELECT (SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM (SELECT 1) ARRAY JOIN [7] AS t) FROM t;

DROP TABLE t;
