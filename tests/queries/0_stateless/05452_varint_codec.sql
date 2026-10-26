-- `VarInt` is experimental: without its dedicated setting it is rejected.
SET enable_varint_codec = 0;
CREATE TABLE varint_codec_disabled (x UInt64 CODEC(Delta, VarInt, ZSTD)) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

SET enable_varint_codec = 1;

-- `VarInt` is a preprocessor, so `CODEC(VarInt)` alone is a suspicious codec declaration.
SET allow_suspicious_codecs = 1;

DROP TABLE IF EXISTS varint_codec_test;

CREATE TABLE varint_codec_test
(
    id UInt64,

    ref_u8 UInt8,
    value_u8 UInt8 CODEC(VarInt),
    ref_u16 UInt16,
    value_u16 UInt16 CODEC(VarInt),
    ref_u32 UInt32,
    value_u32 UInt32 CODEC(VarInt),
    ref_u64 UInt64,
    value_u64 UInt64 CODEC(VarInt),

    ref_i8 Int8,
    value_i8 Int8 CODEC(VarInt),
    ref_i16 Int16,
    value_i16 Int16 CODEC(VarInt),
    ref_i32 Int32,
    value_i32 Int32 CODEC(VarInt),
    ref_i64 Int64,
    value_i64 Int64 CODEC(VarInt),

    ref_u64_delta UInt64,
    value_u64_delta UInt64 CODEC(Delta, VarInt, ZSTD),
    ref_i64_delta Int64,
    value_i64_delta Int64 CODEC(Delta, VarInt, ZSTD),
    ref_dt DateTime,
    value_dt DateTime CODEC(Delta, VarInt, ZSTD),
    ref_dt64 DateTime64(3),
    value_dt64 DateTime64(3) CODEC(Delta, Delta, VarInt, ZSTD)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

-- Sequential values, including a few group boundaries.
INSERT INTO varint_codec_test
SELECT
    number,
    toUInt8(number), toUInt8(number),
    toUInt16(number), toUInt16(number),
    toUInt32(number), toUInt32(number),
    toUInt64(number), toUInt64(number),
    reinterpretAsInt8(toUInt8(number)), reinterpretAsInt8(toUInt8(number)),
    reinterpretAsInt16(toUInt16(number)), reinterpretAsInt16(toUInt16(number)),
    reinterpretAsInt32(toUInt32(number)), reinterpretAsInt32(toUInt32(number)),
    reinterpretAsInt64(toUInt64(number)), reinterpretAsInt64(toUInt64(number)),
    toUInt64(number), toUInt64(number),
    toInt64(number), toInt64(number),
    toDateTime(1700000000 + number * 15), toDateTime(1700000000 + number * 15),
    toDateTime64('2024-01-01 00:00:00', 3) + toIntervalMillisecond(15000 * number),
    toDateTime64('2024-01-01 00:00:00', 3) + toIntervalMillisecond(15000 * number)
FROM numbers(1000);

-- Pseudo-random values of every width.
INSERT INTO varint_codec_test
SELECT
    number,
    toUInt8(sipHash64(number)), toUInt8(sipHash64(number)),
    toUInt16(sipHash64(number)), toUInt16(sipHash64(number)),
    toUInt32(sipHash64(number)), toUInt32(sipHash64(number)),
    sipHash64(number), sipHash64(number),
    reinterpretAsInt8(toUInt8(sipHash64(number))), reinterpretAsInt8(toUInt8(sipHash64(number))),
    reinterpretAsInt16(toUInt16(sipHash64(number))), reinterpretAsInt16(toUInt16(sipHash64(number))),
    reinterpretAsInt32(toUInt32(sipHash64(number))), reinterpretAsInt32(toUInt32(sipHash64(number))),
    reinterpretAsInt64(sipHash64(number)), reinterpretAsInt64(sipHash64(number)),
    sipHash64(number), sipHash64(number),
    reinterpretAsInt64(sipHash64(number)), reinterpretAsInt64(sipHash64(number)),
    toDateTime(sipHash64(number) % 4294967295), toDateTime(sipHash64(number) % 4294967295),
    toDateTime64('2024-01-01 00:00:00', 3) + toIntervalMillisecond(sipHash64(number) % 1000000),
    toDateTime64('2024-01-01 00:00:00', 3) + toIntervalMillisecond(sipHash64(number) % 1000000)
FROM numbers(1000);

-- Boundary values of every width.
INSERT INTO varint_codec_test
    (id, ref_u8, value_u8, ref_i8, value_i8)
VALUES
    (2000, 0, 0, -128, -128),
    (2001, 255, 255, 127, 127);

INSERT INTO varint_codec_test
    (id, ref_u16, value_u16, ref_i16, value_i16)
VALUES
    (2002, 0, 0, -32768, -32768),
    (2003, 65535, 65535, 32767, 32767);

INSERT INTO varint_codec_test
    (id, ref_u32, value_u32, ref_i32, value_i32)
VALUES
    (2004, 0, 0, -2147483648, -2147483648),
    (2005, 4294967295, 4294967295, 2147483647, 2147483647);

INSERT INTO varint_codec_test
    (id, ref_u64, value_u64, ref_i64, value_i64)
VALUES
    (2006, 0, 0, -9223372036854775808, -9223372036854775808),
    (2007, 18446744073709551615, 18446744073709551615, 9223372036854775807, 9223372036854775807);

OPTIMIZE TABLE varint_codec_test FINAL;

SELECT 'u8', count() FROM varint_codec_test WHERE value_u8 != ref_u8;
SELECT 'u16', count() FROM varint_codec_test WHERE value_u16 != ref_u16;
SELECT 'u32', count() FROM varint_codec_test WHERE value_u32 != ref_u32;
SELECT 'u64', count() FROM varint_codec_test WHERE value_u64 != ref_u64;

SELECT 'i8', count() FROM varint_codec_test WHERE value_i8 != ref_i8;
SELECT 'i16', count() FROM varint_codec_test WHERE value_i16 != ref_i16;
SELECT 'i32', count() FROM varint_codec_test WHERE value_i32 != ref_i32;
SELECT 'i64', count() FROM varint_codec_test WHERE value_i64 != ref_i64;

SELECT 'u64 delta', count() FROM varint_codec_test WHERE value_u64_delta != ref_u64_delta;
SELECT 'i64 delta', count() FROM varint_codec_test WHERE value_i64_delta != ref_i64_delta;
SELECT 'DateTime', count() FROM varint_codec_test WHERE value_dt != ref_dt;
SELECT 'DateTime64', count() FROM varint_codec_test WHERE value_dt64 != ref_dt64;

SELECT
    sum(cityHash64(value_u8, value_u16, value_u32, value_u64, value_i8, value_i16, value_i32, value_i64,
                   value_u64_delta, value_i64_delta, value_dt, value_dt64))
    = sum(cityHash64(ref_u8, ref_u16, ref_u32, ref_u64, ref_i8, ref_i16, ref_i32, ref_i64,
                     ref_u64_delta, ref_i64_delta, ref_dt, ref_dt64))
FROM varint_codec_test;

SELECT name, is_compression, is_generic_compression
FROM system.codecs
WHERE name = 'VarInt';

DROP TABLE IF EXISTS varint_codec_test;

-- Invalid declarations.
CREATE TABLE varint_codec_bad_type (x UInt128 CODEC(VarInt)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE varint_codec_bad_type (x Float64 CODEC(VarInt(8))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }
