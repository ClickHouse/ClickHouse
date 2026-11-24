-- A NULL default of arrayShiftLeft/arrayShiftRight is filled with the default of its type, not with the column zero.
SELECT arrayShiftRight(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), 1, CAST(NULL, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')),
       arrayShiftLeft(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), 1, CAST(NULL, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))'));
SELECT arrayShiftLeft(CAST(['x', 'y'], 'Array(Enum16(\'x\' = 1000, \'y\' = 2000))'), -1, materialize(CAST(NULL, 'Nullable(Enum16(\'x\' = 1000, \'y\' = 2000))')));
SELECT arrayShiftRight(CAST(['y'], 'Array(Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2))'), 1, CAST(NULL, 'Nullable(Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2))')),
       arrayShiftRight(CAST(['y'], 'Array(Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2))'), 1);
SELECT number, arrayShiftLeft(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), 1, CAST(if(number = 0, NULL, 'b'), 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')) FROM numbers(2) ORDER BY number;
SELECT arrayShiftRight(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), 1, materialize(CAST('b', 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')));
SELECT arrayShiftRight(CAST([('y', 5), ('y', 6)], 'Array(Tuple(e Enum8(\'x\' = 1, \'y\' = 2), n UInt8))'), 1, CAST(NULL, 'Nullable(Tuple(e Enum8(\'x\' = 1, \'y\' = 2), n UInt8))')) SETTINGS enable_nullable_tuple_type = 1;
SELECT arrayShiftRight(CAST([tuple('{"a":"y"}'), tuple('{"a":"y"}')], 'Array(Tuple(j JSON(a Enum8(\'x\' = 1, \'y\' = 2))))'), 1, CAST(NULL, 'Nullable(Tuple(j JSON(a Enum8(\'x\' = 1, \'y\' = 2))))')) SETTINGS enable_nullable_tuple_type = 1;
SELECT arrayShiftRight(['x', 'y'], 1, CAST(NULL, 'Nullable(Enum8(\'a\' = 1))'));
SELECT arrayShiftRight(CAST(['a', 'a'], 'Array(Nullable(Enum8(\'a\' = 1)))'), 1, CAST(NULL, 'Nullable(Enum8(\'a\' = 1))'));

DROP TABLE IF EXISTS t_array_shift_enum_src;
DROP TABLE IF EXISTS t_array_shift_enum_dst;
CREATE TABLE t_array_shift_enum_src (id UInt8, a Array(Enum8('a' = 1, 'b' = 2)), d Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = Memory;
INSERT INTO t_array_shift_enum_src VALUES (1, ['a', 'b'], 'b'), (2, ['a', 'b'], NULL);
SELECT id, arrayShiftRight(a, 1, d), arrayShiftLeft(a, 1, d) FROM t_array_shift_enum_src ORDER BY id;
CREATE TABLE t_array_shift_enum_dst (id UInt8, a Array(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array_shift_enum_dst SELECT id, arrayShiftRight(a, 1, d) FROM t_array_shift_enum_src;
SELECT id, a, CAST(a, 'Array(Int8)') FROM t_array_shift_enum_dst ORDER BY id;
DROP TABLE t_array_shift_enum_src;
DROP TABLE t_array_shift_enum_dst;
