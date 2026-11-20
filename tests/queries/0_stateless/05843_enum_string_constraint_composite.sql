-- `Array(Enum)` and `Array(String)` compare by the names, two `Enum` arrays compare by the values.
-- The constraint graph must not chain the two orders, also inside a tuple or a map.

DROP TABLE IF EXISTS t_enum_array_bound;
DROP TABLE IF EXISTS t_enum_tuple_array_bound;
DROP TABLE IF EXISTS t_enum_map_bound;
DROP TABLE IF EXISTS t_enum_array_equal;
DROP TABLE IF EXISTS t_enum_array_atom;

SET convert_query_to_cnf = 1, optimize_use_implicit_projections = 0, optimize_substitute_columns = 0;

CREATE TABLE t_enum_array_bound (a Array(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a <= CAST(['a'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))')) ENGINE = Memory;
INSERT INTO t_enum_array_bound VALUES (['z']);
-- ['z'] < ['m'] by the names is false
SELECT 'array', count() FROM t_enum_array_bound WHERE a < ['m'] SETTINGS optimize_using_constraints = 0;
SELECT 'array', count() FROM t_enum_array_bound WHERE a < ['m'] SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_tuple_array_bound (a Tuple(Array(Enum8('z' = 1, 'a' = 2, 'm' = 3))), CONSTRAINT c CHECK a <= CAST(tuple(['a']), 'Tuple(Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3)))')) ENGINE = Memory;
INSERT INTO t_enum_tuple_array_bound VALUES ((['z']));
SELECT 'tuple of array', count() FROM t_enum_tuple_array_bound WHERE a < tuple(['m']) SETTINGS optimize_using_constraints = 0;
SELECT 'tuple of array', count() FROM t_enum_tuple_array_bound WHERE a < tuple(['m']) SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_map_bound (m Map(String, Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK m <= CAST(map('k', 'a'), 'Map(String, Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))')) ENGINE = Memory;
INSERT INTO t_enum_map_bound VALUES (map('k', 'z'));
SELECT 'map', count() FROM t_enum_map_bound WHERE m < map('k', 'm') SETTINGS optimize_using_constraints = 0;
SELECT 'map', count() FROM t_enum_map_bound WHERE m < map('k', 'm') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_array_equal (a Array(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a = CAST(['a'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))')) ENGINE = Memory;
INSERT INTO t_enum_array_equal VALUES (['a']);
SELECT 'array equals', count() FROM t_enum_array_equal WHERE a = ['a'] SETTINGS optimize_using_constraints = 0;
SELECT 'array equals', count() FROM t_enum_array_equal WHERE a = ['a'] SETTINGS optimize_using_constraints = 1;

-- `a >= b` is by the names, `a >= CAST(['m'], ...)` is by the values
CREATE TABLE t_enum_array_atom (a Array(Enum8('z' = 1, 'a' = 2, 'm' = 3)), b Array(String), CONSTRAINT c1 CHECK a >= b, CONSTRAINT c2 CHECK b >= ['a']) ENGINE = Memory;
INSERT INTO t_enum_array_atom VALUES (['z'], ['b']);
SELECT 'array atom', count() FROM t_enum_array_atom WHERE a >= CAST(['m'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'array atom', count() FROM t_enum_array_atom WHERE a >= CAST(['m'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

DROP TABLE t_enum_array_bound;
DROP TABLE t_enum_tuple_array_bound;
DROP TABLE t_enum_map_bound;
DROP TABLE t_enum_array_equal;
DROP TABLE t_enum_array_atom;
