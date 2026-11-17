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
