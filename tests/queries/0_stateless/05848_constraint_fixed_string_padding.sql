-- a `FixedString` compared with a string of another size is padded with zero bytes, so 'a' = toFixedString('a', 2)

SET convert_query_to_cnf = 1, optimize_using_constraints = 1;

DROP TABLE IF EXISTS t_string;
DROP TABLE IF EXISTS t_fixed_string;
DROP TABLE IF EXISTS t_string_fixed_string;
DROP TABLE IF EXISTS t_same_fixed_string;

CREATE TABLE t_string (s String, CONSTRAINT c CHECK s = 'a') ENGINE = Memory;
INSERT INTO t_string VALUES ('a');
SELECT 'string', count() FROM t_string WHERE s = toFixedString('a', 2) SETTINGS optimize_using_constraints = 0;
SELECT 'string', count() FROM t_string WHERE s = toFixedString('a', 2);

CREATE TABLE t_fixed_string (f FixedString(2), CONSTRAINT c CHECK f = 'a') ENGINE = Memory;
INSERT INTO t_fixed_string VALUES ('a');
SELECT 'fixed string', count() FROM t_fixed_string WHERE f = toFixedString('a', 2) SETTINGS optimize_using_constraints = 0;
SELECT 'fixed string', count() FROM t_fixed_string WHERE f = toFixedString('a', 2);
SELECT 'fixed string less', count() FROM t_fixed_string WHERE f < 'a\0\0' SETTINGS optimize_using_constraints = 0;
SELECT 'fixed string less', count() FROM t_fixed_string WHERE f < 'a\0\0';

-- `CAST` of `f` to `String` drops the zero byte that `s` keeps
CREATE TABLE t_string_fixed_string (s String, f FixedString(2), CONSTRAINT c CHECK s = f) ENGINE = MergeTree ORDER BY f;
INSERT INTO t_string_fixed_string VALUES ('a', 'a'), ('a\0', 'a');
SELECT 'substitution', arraySort(groupArray(hex(s))) FROM t_string_fixed_string WHERE f != 'x' SETTINGS optimize_substitute_columns = 0;
SELECT 'substitution', arraySort(groupArray(hex(s))) FROM t_string_fixed_string WHERE f != 'x' SETTINGS optimize_substitute_columns = 1;

-- the same `FixedString` type on both sides has no padding, so the graph still folds the filter
CREATE TABLE t_same_fixed_string (f FixedString(2), CONSTRAINT c CHECK f = toFixedString('a', 2)) ENGINE = Memory;
INSERT INTO t_same_fixed_string VALUES ('a');
SELECT 'same fixed string', count() FROM t_same_fixed_string WHERE f = toFixedString('b', 2);
SELECT 'same fixed string', count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_same_fixed_string WHERE f = toFixedString('b', 2)) WHERE explain LIKE '%toFixedString%';

DROP TABLE t_string;
DROP TABLE t_fixed_string;
DROP TABLE t_string_fixed_string;
DROP TABLE t_same_fixed_string;
