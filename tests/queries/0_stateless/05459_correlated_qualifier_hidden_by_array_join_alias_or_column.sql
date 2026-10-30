-- Under `analyzer_alias_hides_table_name` the alias of `t AS c` hides the name `t` from a qualifier when an
-- enclosing query gives `t.val` another reading. Besides a table name, that reading can come from an
-- `ARRAY JOIN` alias or from a column with a subcolumn `val`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS u;
DROP TABLE IF EXISTS w;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

CREATE TABLE u (t Tuple(val UInt64)) ENGINE = Memory;
INSERT INTO u VALUES ((10));

CREATE TABLE w (t UInt64) ENGINE = Memory;
INSERT INTO w VALUES (1);

SELECT 'ARRAY JOIN alias';
SELECT count() FROM numbers(1) ARRAY JOIN [CAST(tuple(10), 'Tuple(val UInt64)')] AS t
WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val)
SETTINGS analyzer_alias_hides_table_name = 1;
SELECT count() FROM numbers(1) ARRAY JOIN [CAST(tuple(10), 'Tuple(val UInt64)')] AS x
WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > x.val);
-- The previous reading, which is the default.
SELECT count() FROM numbers(1) ARRAY JOIN [CAST(tuple(10), 'Tuple(val UInt64)')] AS t
WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val);

SELECT 'Tuple column';
SELECT count() FROM u WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val)
SETTINGS analyzer_alias_hides_table_name = 1;
SELECT count() FROM u AS o WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > o.t.val);
SELECT count() FROM u WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val);

SELECT 'a column without such a subcolumn does not hide the name';
SELECT count() FROM w WHERE EXISTS (SELECT 1 FROM t AS c WHERE c.val > t.val)
SETTINGS analyzer_alias_hides_table_name = 1;

DROP TABLE t;
DROP TABLE u;
DROP TABLE w;
