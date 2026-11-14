-- Under `analyzer_alias_hides_table_name`, when both sides of a `JOIN` of an enclosing query have a column `t`, the
-- column the lookup there actually binds decides whether the name of `t AS c` is hidden from the qualifier of `t.*`:
-- the left side with `single_join_prefer_left_table`, otherwise the identifier is ambiguous, and with
-- `prefer_column_name_to_alias` an expression alias `t` takes over.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'scalar left column wins over tuple right column';
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT 5 AS t) AS a CROSS JOIN (SELECT tuple(7) AS t) AS b;

SELECT 'tuple left column wins over scalar right column';
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT tuple(7) AS t) AS a CROSS JOIN (SELECT 5 AS t) AS b;

SELECT 'ambiguous joined columns, the alias takes over';
WITH (8,) AS t
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT 5 AS t) AS a CROSS JOIN (SELECT 6 AS t) AS b
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

WITH (8,) AS t
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT 5 AS t) AS a CROSS JOIN (SELECT 6 AS t) AS b
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 0;

DROP TABLE t;
