-- Under `analyzer_alias_hides_table_name` the alias of `t AS c` hides the name `t` from a qualifier when an
-- enclosing query gives `t.val` another reading. That reading can come from an expression alias of the
-- enclosing query as well, either in its projection or in its `WITH` clause.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'projection alias';
SELECT CAST(tuple(number + 10), 'Tuple(val UInt64)') AS t, (SELECT count() FROM t AS c WHERE c.val > t.val) AS n FROM numbers(1);
SELECT CAST(tuple(number + 10), 'Tuple(val UInt64)') AS x, (SELECT count() FROM t AS c WHERE c.val > x.val) AS n FROM numbers(1);

SELECT 'WITH alias';
WITH CAST(tuple(10), 'Tuple(val UInt64)') AS t SELECT EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val);
WITH CAST(tuple(10), 'Tuple(val UInt64)') AS x SELECT EXISTS (SELECT 1 FROM t AS c WHERE c.val > x.val);

SELECT 'WITH alias, enable_global_with_statement = 0';
-- The enclosing alias is not visible to the subquery, so `t.val` keeps addressing `t AS c`.
WITH CAST(tuple(10), 'Tuple(val UInt64)') AS t SELECT EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val) SETTINGS enable_global_with_statement = 0;

SELECT 'alias of the subquery itself';
-- The alias of the expression being resolved does not count, so `t.val` keeps addressing `t AS c`.
SELECT (SELECT count() FROM t AS c WHERE c.val > t.val) AS t FROM numbers(1);

SELECT 'constant alias without the element';
-- A constant alias has a known type; when it has no element `val`, it gives `t.val` no reading, so `t.val`
-- keeps addressing `t AS c`, whichever order the projection is resolved in.
SELECT 1 AS t, (SELECT count() FROM t AS c WHERE t.val > 15) AS n;
SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n, 1 AS t;
WITH 1 AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15);
WITH (1, 2) AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15);
WITH [1] AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15);

SELECT 'disabled';
SELECT CAST(tuple(number + 10), 'Tuple(val UInt64)') AS t, (SELECT count() FROM t AS c WHERE c.val > t.val) AS n FROM numbers(1) SETTINGS analyzer_alias_hides_table_name = 0;

DROP TABLE t;
