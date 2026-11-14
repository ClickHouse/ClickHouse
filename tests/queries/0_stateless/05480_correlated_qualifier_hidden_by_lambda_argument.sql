-- Under `analyzer_alias_hides_table_name` the alias of `t AS c` hides the name `t` from a qualifier when an
-- enclosing scope gives `t.val` another reading. A lambda argument of an enclosing lambda is such a reading,
-- as long as it has the element `val`.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'lambda argument';
WITH t -> (SELECT count() FROM t AS c WHERE c.val > t.val) AS f SELECT f(CAST(tuple(10), 'Tuple(val UInt64)'));
WITH x -> (SELECT count() FROM t AS c WHERE c.val > x.val) AS f SELECT f(CAST(tuple(10), 'Tuple(val UInt64)'));
WITH t -> (SELECT count() FROM t AS c WHERE c.val > t.val) AS f SELECT f(CAST(tuple(10), 'Tuple(val UInt64)')) SETTINGS enable_global_with_statement = 0;

SELECT 'lambda argument without the element';
-- The argument has no element `val`, so `t.val` keeps addressing `t AS c`.
WITH t -> (SELECT count() FROM t AS c WHERE t.val > 15) AS f SELECT f(CAST(tuple(10), 'Tuple(other UInt64)'));
WITH t -> (SELECT count() FROM t AS c WHERE t.val > 15) AS f SELECT f(1);

SELECT 'lambda argument without the element shadows an outer table';
-- The argument binds `t` and stops the lookup of `t.val` before it reaches the outer `FROM t`, so that table
-- gives `t.val` no reading and the inner `t AS c` keeps the name.
WITH t -> (SELECT count() FROM t AS c WHERE t.val > 15) AS f SELECT f(1) FROM t;
WITH t -> (SELECT count() FROM t AS c WHERE t.val > 15) AS f SELECT f(CAST(tuple(10), 'Tuple(other UInt64)')) FROM t;

SELECT 'disabled';
WITH t -> (SELECT count() FROM t AS c WHERE c.val > t.val) AS f SELECT f(CAST(tuple(10), 'Tuple(val UInt64)')) SETTINGS analyzer_alias_hides_table_name = 0;

DROP TABLE t;
