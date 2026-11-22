-- a tuple in an ordered constraint can hold a NULL or a NaN element, so it is not always equal to itself

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

DROP TABLE IF EXISTS t_self_nullable;
DROP TABLE IF EXISTS t_self_nan;
DROP TABLE IF EXISTS t_self_equal;

CREATE TABLE t_self_nullable (a Tuple(Int32, Nullable(Int32)), b Tuple(Int32, Nullable(Int32)), CONSTRAINT c CHECK a < b) ENGINE = Memory;
INSERT INTO t_self_nullable VALUES ((0, NULL), (1, 0));
SELECT 'nullable a = a', count() FROM t_self_nullable WHERE a = a SETTINGS optimize_using_constraints = 0;
SELECT 'nullable a = a', count() FROM t_self_nullable WHERE a = a SETTINGS optimize_using_constraints = 1;
SELECT 'nullable a <= a', count() FROM t_self_nullable WHERE a <= a SETTINGS optimize_using_constraints = 0;
SELECT 'nullable a <= a', count() FROM t_self_nullable WHERE a <= a SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_self_nan (a Tuple(Int32, Float64), b Tuple(Int32, Float64), CONSTRAINT c CHECK a < b) ENGINE = Memory;
INSERT INTO t_self_nan VALUES ((0, nan), (1, 0));
SELECT 'nan a = a', count() FROM t_self_nan WHERE a = a SETTINGS optimize_using_constraints = 0;
SELECT 'nan a = a', count() FROM t_self_nan WHERE a = a SETTINGS optimize_using_constraints = 1;
SELECT 'nan a <= a', count() FROM t_self_nan WHERE a <= a SETTINGS optimize_using_constraints = 0;
SELECT 'nan a <= a', count() FROM t_self_nan WHERE a <= a SETTINGS optimize_using_constraints = 1;
SELECT 'nan a != a', count() FROM t_self_nan WHERE a != a SETTINGS optimize_using_constraints = 0;
SELECT 'nan a != a', count() FROM t_self_nan WHERE a != a SETTINGS optimize_using_constraints = 1;

-- an equality rules out a NULL and a NaN element
CREATE TABLE t_self_equal (a Tuple(Int32, Nullable(Float64)), b Tuple(Int32, Nullable(Float64)), CONSTRAINT c CHECK a = b) ENGINE = Memory;
INSERT INTO t_self_equal VALUES ((0, 1), (0, 1));
SELECT 'equal a = b', count() FROM t_self_equal WHERE a = b SETTINGS optimize_using_constraints = 0;
SELECT 'equal a = b', count() FROM t_self_equal WHERE a = b SETTINGS optimize_using_constraints = 1;

DROP TABLE t_self_nullable;
DROP TABLE t_self_nan;
DROP TABLE t_self_equal;
