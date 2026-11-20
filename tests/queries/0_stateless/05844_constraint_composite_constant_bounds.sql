-- the constraint graph must not order array, tuple and map constants by their fields

DROP TABLE IF EXISTS t_enum_array_le;
DROP TABLE IF EXISTS t_enum_array_eq;
DROP TABLE IF EXISTS t_enum_tuple_le;
DROP TABLE IF EXISTS t_enum_map_le;
DROP TABLE IF EXISTS t_number_tuple_le;
DROP TABLE IF EXISTS t_number_array_le;
DROP TABLE IF EXISTS t_string_array_bound;
DROP TABLE IF EXISTS t_string_tuple_bound;

SET convert_query_to_cnf = 1, optimize_use_implicit_projections = 0, optimize_substitute_columns = 0, optimize_append_index = 0;

CREATE TABLE t_enum_array_le (a Array(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a <= [2]) ENGINE = Memory;
INSERT INTO t_enum_array_le VALUES (['z']);
SELECT 'enum array le', count() FROM t_enum_array_le WHERE a < CAST(['z'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'enum array le', count() FROM t_enum_array_le WHERE a < CAST(['z'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_array_eq (a Array(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a = [1]) ENGINE = Memory;
INSERT INTO t_enum_array_eq VALUES (['z']);
SELECT 'enum array eq', count() FROM t_enum_array_eq WHERE a = CAST(['z'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'enum array eq', count() FROM t_enum_array_eq WHERE a = CAST(['z'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_tuple_le (a Tuple(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a <= tuple(2)) ENGINE = Memory;
INSERT INTO t_enum_tuple_le VALUES (tuple('z'));
SELECT 'enum tuple le', count() FROM t_enum_tuple_le WHERE a < CAST(tuple('z'), 'Tuple(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'enum tuple le', count() FROM t_enum_tuple_le WHERE a < CAST(tuple('z'), 'Tuple(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_map_le (m Map(String, Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK m <= map('k', 2)) ENGINE = Memory;
INSERT INTO t_enum_map_le VALUES (map('k', 'z'));
SELECT 'enum map le', count() FROM t_enum_map_le WHERE m < CAST(map('k', 'z'), 'Map(String, Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'enum map le', count() FROM t_enum_map_le WHERE m < CAST(map('k', 'z'), 'Map(String, Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_number_tuple_le (t Tuple(UInt8, UInt8), CONSTRAINT c CHECK t <= (1, 2)) ENGINE = Memory;
INSERT INTO t_number_tuple_le VALUES ((0, 0));
SELECT 'number tuple le', count() FROM t_number_tuple_le WHERE t < (-1, 5) SETTINGS optimize_using_constraints = 0;
SELECT 'number tuple le', count() FROM t_number_tuple_le WHERE t < (-1, 5) SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_number_array_le (a Array(Int16), CONSTRAINT c CHECK a <= [1]) ENGINE = Memory;
INSERT INTO t_number_array_le VALUES ([0]);
SELECT 'number array le', count() FROM t_number_array_le WHERE a < [-1] SETTINGS optimize_using_constraints = 0;
SELECT 'number array le', count() FROM t_number_array_le WHERE a < [-1] SETTINGS optimize_using_constraints = 1;

-- a string bound of an array or a tuple column must not fail the query
CREATE TABLE t_string_array_bound (a Array(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a <= '[\'a\']') ENGINE = Memory;
INSERT INTO t_string_array_bound VALUES (['z']);
SELECT 'string array bound', count() FROM t_string_array_bound WHERE a < CAST(['m'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'string array bound', count() FROM t_string_array_bound WHERE a < CAST(['m'], 'Array(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_string_tuple_bound (a Tuple(Enum8('z' = 1, 'a' = 2, 'm' = 3)), CONSTRAINT c CHECK a <= '(\'a\')') ENGINE = Memory;
INSERT INTO t_string_tuple_bound VALUES (tuple('z'));
SELECT 'string tuple bound', count() FROM t_string_tuple_bound WHERE a < CAST(tuple('m'), 'Tuple(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 0;
SELECT 'string tuple bound', count() FROM t_string_tuple_bound WHERE a < CAST(tuple('m'), 'Tuple(Enum8(\'z\' = 1, \'a\' = 2, \'m\' = 3))') SETTINGS optimize_using_constraints = 1;

DROP TABLE t_enum_array_le;
DROP TABLE t_enum_array_eq;
DROP TABLE t_enum_tuple_le;
DROP TABLE t_enum_map_le;
DROP TABLE t_number_tuple_le;
DROP TABLE t_number_array_le;
DROP TABLE t_string_array_bound;
DROP TABLE t_string_tuple_bound;
