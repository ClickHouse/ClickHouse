-- A number that is not a value of the target `Enum` gives NULL in `accurateCastOrNull` and the default in `accurateCastOrDefault`.

SELECT accurateCastOrNull(toUInt8(1), 'Enum8(\'a\' = 1)'), accurateCastOrNull(toUInt8(2), 'Enum8(\'a\' = 1)');
SELECT accurateCastOrNull(toInt16(300), 'Enum16(\'a\' = 1)'), accurateCastOrNull(toInt16(300), 'Enum8(\'a\' = 1)');
SELECT accurateCastOrNull(toInt64(-1), 'Enum16(\'a\' = -1)'), accurateCastOrNull(toUInt64(65535), 'Enum16(\'a\' = -1)');
SELECT accurateCastOrNull(toFloat64(1), 'Enum8(\'a\' = 1)'), accurateCastOrNull(toFloat64(2), 'Enum8(\'a\' = 1)');
SELECT accurateCastOrNull(toFloat64(1.5), 'Enum8(\'a\' = 1)'), accurateCastOrNull(nan, 'Enum8(\'a\' = 0)'), accurateCastOrNull(toFloat32(inf), 'Enum16(\'a\' = 0)');
SELECT accurateCastOrNull(tuple(materialize(toUInt8(2))), 'Tuple(Enum8(\'a\' = 1))');
SELECT accurateCastOrNull(tuple(materialize(toUInt8(1))), 'Tuple(Enum8(\'a\' = 1))');

SELECT number, accurateCastOrNull(number, 'Enum8(\'a\' = 1, \'b\' = 3)') FROM numbers(5);
SELECT number, accurateCastOrNull(number, 'Nullable(Enum16(\'a\' = 1, \'b\' = 3))') FROM numbers(5);
SELECT x, accurateCastOrNull(x, 'Enum8(\'a\' = 1)') FROM (SELECT arrayJoin([1, NULL, 2]::Array(Nullable(UInt8))) AS x);

SELECT accurateCastOrDefault(toUInt8(2), 'Enum8(\'a\' = 1, \'b\' = 2)'), accurateCastOrDefault(toUInt8(3), 'Enum8(\'a\' = 1, \'b\' = 2)');
SELECT accurateCastOrDefault(toInt16(300), 'Enum16(\'a\' = 1)', 'a'::Enum16('a' = 1));

SELECT accurateCast(toUInt8(2), 'Enum8(\'a\' = 1)'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(toUInt8(2), 'Enum8(\'a\' = 1)'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(toInt16(300), 'Enum8(\'a\' = 1)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(materialize(nan), 'Enum8(\'a\' = 0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(materialize(nan), 'Enum16(\'a\' = 0)'); -- { serverError CANNOT_CONVERT_TYPE }

-- Array and map elements cannot hold NULL, so they convert strictly.
SET enable_nullable_tuple_type = 1;
SELECT accurateCastOrNull(tuple(materialize([toUInt8(1)])), 'Nullable(Tuple(Array(Enum8(\'a\' = 1))))');
SELECT accurateCastOrNull(tuple(materialize(map('k', toUInt8(1)))), 'Nullable(Tuple(Map(String, Enum8(\'a\' = 1))))');
SELECT accurateCastOrNull(tuple(materialize([toUInt8(2)])), 'Nullable(Tuple(Array(Enum8(\'a\' = 1))))'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT accurateCastOrNull(tuple(materialize(toUInt8(2))), 'Nullable(Tuple(Enum8(\'a\' = 1)))');
SELECT accurateCastOrNull(tuple(materialize(toUInt8(1))), 'Nullable(Tuple(Enum8(\'a\' = 1)))');

-- Both accurate modes reject a fractional number, `CAST` truncates it.
SELECT accurateCast(materialize(toFloat64(1.5)), 'Enum8(\'a\' = 1)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(materialize(toFloat32(1.5)), 'Enum16(\'a\' = 1)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize([toFloat64(1.5)])), 'Nullable(Tuple(Array(Enum8(\'a\' = 1))))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize(map('k', toFloat64(1.5)))), 'Nullable(Tuple(Map(String, Enum8(\'a\' = 1))))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(materialize(toFloat64(1)), 'Enum8(\'a\' = 1)'), CAST(materialize(toFloat64(1.5)), 'Enum8(\'a\' = 1)');

-- With `check_conversion_from_numbers_to_enum = 0`, `accurateCastOrNull` still checks the range.
SELECT accurateCastOrNull(materialize(toUInt16(257)), 'Enum8(\'a\' = 1)') SETTINGS check_conversion_from_numbers_to_enum = 0;

-- A number that is not a value of the `Enum` key of a set is not a member.
SELECT materialize(toUInt16(257)) IN (SELECT CAST('a', 'Enum8(\'a\' = 1)')), materialize(toUInt16(1)) IN (SELECT CAST('a', 'Enum8(\'a\' = 1)'));
SELECT tuple(materialize(toUInt16(257))) IN (SELECT tuple(CAST('a', 'Enum8(\'a\' = 1)'))), tuple(materialize(toUInt16(1))) IN (SELECT tuple(CAST('a', 'Enum8(\'a\' = 1)')));
SELECT materialize(toFloat64(1.5)) IN (SELECT CAST('a', 'Enum8(\'a\' = 1)')), materialize(nan) IN (SELECT CAST('a', 'Enum16(\'a\' = 1)'));

-- Array and map keys of a set convert with `accurateCast`, so a number that does not fit throws like it does for numeric keys.
SELECT materialize([toUInt16(2)]) IN (SELECT [CAST('a', 'Enum8(\'a\' = 1)')]), materialize([toUInt16(1)]) IN (SELECT [CAST('a', 'Enum8(\'a\' = 1)')]);
SELECT materialize(map('k', toUInt16(2))) IN (SELECT map('k', CAST('a', 'Enum8(\'a\' = 1)'))), materialize(map('k', toUInt16(1))) IN (SELECT map('k', CAST('a', 'Enum8(\'a\' = 1)')));
SELECT materialize([toUInt16(257)]) IN (SELECT [CAST('a', 'Enum8(\'a\' = 1)')]); -- { serverError CANNOT_CONVERT_TYPE }
SELECT materialize([1.5]) IN (SELECT [CAST('a', 'Enum8(\'a\' = 1)')]); -- { serverError CANNOT_CONVERT_TYPE }
SELECT materialize(map('k', toUInt16(257))) IN (SELECT map('k', CAST('a', 'Enum8(\'a\' = 1)'))); -- { serverError CANNOT_CONVERT_TYPE }
SELECT tuple(materialize([toUInt16(257)])) IN (SELECT tuple([CAST('a', 'Enum8(\'a\' = 1)')])); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(materialize(toUInt16(257)), 'Enum8(\'a\' = 1)') SETTINGS check_conversion_from_numbers_to_enum = 0; -- { serverError CANNOT_CONVERT_TYPE }

-- With `check_conversion_from_numbers_to_enum = 0`, a set key can hold a value that is not in the `Enum`, and a number with that value is a member.
SELECT materialize(toUInt8(2)) IN (SELECT CAST(materialize(toUInt8(2)), 'Enum8(\'a\' = 1)')), materialize(toUInt8(2)) NOT IN (SELECT CAST(materialize(toUInt8(2)), 'Enum8(\'a\' = 1)')) SETTINGS check_conversion_from_numbers_to_enum = 0;
SELECT materialize(toUInt16(2)) IN (SELECT CAST(materialize(toUInt8(2)), 'Enum16(\'a\' = 1)')) SETTINGS check_conversion_from_numbers_to_enum = 0;
SELECT tuple(materialize(toUInt8(2))) IN (SELECT tuple(CAST(materialize(toUInt8(2)), 'Enum8(\'a\' = 1)'))) SETTINGS check_conversion_from_numbers_to_enum = 0;
