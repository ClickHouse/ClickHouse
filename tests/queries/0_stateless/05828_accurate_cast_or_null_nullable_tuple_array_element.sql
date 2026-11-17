-- accurateCastOrNull / accurateCastOrDefault to a Nullable(Tuple) whose element is an Array or a Map:
-- the elements of the Array or the Map convert to their declared types, as with accurateCast.
-- https://github.com/ClickHouse/ClickHouse/issues/132281

SET enable_nullable_tuple_type = 1;

SELECT accurateCastOrNull(tuple(materialize([CAST('1', 'Int128')])), 'Nullable(Tuple(Array(UInt64)))');

SELECT accurateCastOrNull(tuple(materialize([toUInt16(1)])), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrNull(tuple(materialize(map('a', toUInt16(1)))), 'Nullable(Tuple(Map(String, UInt8)))');
SELECT accurateCastOrNull(tuple(materialize(map(toUInt16(1), 'a'))), 'Nullable(Tuple(Map(UInt8, String)))');
SELECT accurateCastOrNull(tuple(materialize([tuple(toUInt16(1))])), 'Nullable(Tuple(Array(Tuple(UInt8))))');
SELECT accurateCastOrNull(tuple(materialize([[toUInt16(1)]])), 'Nullable(Tuple(Array(Array(UInt8))))');
SELECT accurateCastOrNull(tuple(materialize([CAST(toUInt16(1), 'Variant(String, UInt16)')])), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrNull(tuple(materialize([toUInt16(1)::Dynamic])), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrNull(tuple(materialize([toInt64(1)])), 'Nullable(Tuple(Array(Float64)))');
SELECT accurateCastOrNull(tuple(materialize([toUInt16(1)])), 'Nullable(Tuple(Array(Date)))');
SELECT accurateCastOrNull(tuple(materialize([toUInt32(1)])), 'Nullable(Tuple(Array(IPv4)))');
SELECT accurateCastOrNull(tuple(materialize([toUInt16(1)])), 'Nullable(Tuple(Array(Decimal32(2))))');
SELECT accurateCastOrNull(tuple(materialize([toUInt16(1)])), 'Nullable(Tuple(Array(DateTime64(3, \'UTC\'))))');
SELECT accurateCastOrNull(tuple(materialize(['ab'])), 'Nullable(Tuple(Array(FixedString(2))))');

-- An element that does not convert fails as with accurateCast.
SELECT accurateCastOrNull(tuple(materialize([toUInt16(300)])), 'Nullable(Tuple(Array(UInt8)))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize(map('a', toUInt16(300)))), 'Nullable(Tuple(Map(String, UInt8)))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize([toInt64(-1)])), 'Nullable(Tuple(Array(DateTime)))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize([toInt32(1)])), 'Nullable(Tuple(Array(UUID)))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize([toIPv4('1.2.3.4')])), 'Nullable(Tuple(Array(Decimal32(2))))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize(['abc'])), 'Nullable(Tuple(Array(FixedString(2))))'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT accurateCastOrNull(tuple(materialize([today()])), 'Nullable(Tuple(Array(Enum8(\'a\' = 1))))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize([NULL])), 'Nullable(Tuple(Array(UInt8)))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(tuple(materialize([toUInt16(1)])), 'Nullable(Tuple(Array(Nothing)))'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCastOrDefault(tuple(materialize([toUInt16(1), toUInt16(2)])), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrDefault(tuple(materialize(map('a', toUInt16(7)))), 'Nullable(Tuple(Map(String, UInt8)))');
SELECT accurateCastOrDefault(tuple(materialize([toUInt16(1), toUInt16(2), toUInt16(300)])), 'Nullable(Tuple(Array(UInt8)))'); -- { serverError CANNOT_CONVERT_TYPE }

-- A value that does not fit is still reported as NULL at the top level, in a direct tuple element and in a Nullable array element.
SELECT accurateCastOrNull(materialize(toUInt16(300)), 'UInt8');
SELECT accurateCastOrNull(tuple(materialize(toUInt16(300))), 'Nullable(Tuple(UInt8))');
SELECT accurateCastOrNull(tuple(materialize([toUInt16(300)])), 'Nullable(Tuple(Array(Nullable(UInt8))))');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 3)), 'FixedString(2)');
SELECT accurateCastOrNull(materialize([1e300, 1]::Array(Float64)), 'QBit(Float32, 2)');
SELECT accurateCastOrNull(materialize([1, NULL]::Array(Nullable(Float32))), 'QBit(Float32, 2)');
SELECT accurateCastOrNull(materialize([1, 2]::Array(Float64)), 'QBit(Float32, 2)');
