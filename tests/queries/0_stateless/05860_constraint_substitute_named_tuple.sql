-- a named tuple equal to a tuple with other element names must keep its element order after substitution

SET convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_append_index = 0;

DROP TABLE IF EXISTS t_sub_reversed;
DROP TABLE IF EXISTS t_sub_partial;
DROP TABLE IF EXISTS t_sub_array;

CREATE TABLE t_sub_reversed (a Tuple(x UInt64, y UInt64), b Tuple(y UInt64, x UInt64), CONSTRAINT c CHECK a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_sub_reversed VALUES ((1, 2), (1, 2));
SELECT 'reversed projection', a FROM t_sub_reversed WHERE b.1 > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'reversed projection', a FROM t_sub_reversed WHERE b.1 > 0 SETTINGS optimize_substitute_columns = 1;
SELECT 'reversed filter', count() FROM t_sub_reversed WHERE toString(a) = '(1,2)' SETTINGS optimize_substitute_columns = 0;
SELECT 'reversed filter', count() FROM t_sub_reversed WHERE toString(a) = '(1,2)' SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_sub_partial (a Tuple(x UInt64, y UInt64), b Tuple(x UInt64, z UInt64), CONSTRAINT c CHECK a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_sub_partial VALUES ((1, 2), (1, 2));
SELECT 'partial projection', a FROM t_sub_partial WHERE b.1 > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'partial projection', a FROM t_sub_partial WHERE b.1 > 0 SETTINGS optimize_substitute_columns = 1;
SELECT 'partial filter', count() FROM t_sub_partial WHERE toString(a) = '(1,2)' SETTINGS optimize_substitute_columns = 0;
SELECT 'partial filter', count() FROM t_sub_partial WHERE toString(a) = '(1,2)' SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_sub_array (a Array(Tuple(x UInt64, y UInt64)), b Array(Tuple(y UInt64, x UInt64)), CONSTRAINT c CHECK a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_sub_array VALUES ([(1, 2)], [(1, 2)]);
SELECT 'array projection', a FROM t_sub_array WHERE length(b) > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'array projection', a FROM t_sub_array WHERE length(b) > 0 SETTINGS optimize_substitute_columns = 1;
SELECT 'array filter', count() FROM t_sub_array WHERE toString(a) = '[(1,2)]' SETTINGS optimize_substitute_columns = 0;
SELECT 'array filter', count() FROM t_sub_array WHERE toString(a) = '[(1,2)]' SETTINGS optimize_substitute_columns = 1;

DROP TABLE t_sub_reversed;
DROP TABLE t_sub_partial;
DROP TABLE t_sub_array;
