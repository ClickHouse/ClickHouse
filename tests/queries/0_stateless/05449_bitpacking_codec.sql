-- Checks the `BitPacking` codec, which packs the integers of a column in chunks of 64 KiB, each as the differences from
-- the least integer of the chunk in as many bits as the largest difference takes, before another codec compresses them.
-- It is a codec of data preparation: on its own, or in front of a general-purpose codec.

SET enable_bitpacking_codec = 1;

DROP TABLE IF EXISTS bitpacking;
DROP TABLE IF EXISTS bitpacking_plain;

CREATE TABLE bitpacking
(
    k UInt32 CODEC(BitPacking),
    i8 Int8 CODEC(BitPacking),
    u8 UInt8 CODEC(BitPacking, ZSTD),
    i16 Int16 CODEC(BitPacking, LZ4),
    u16 UInt16 CODEC(BitPacking),
    i32 Int32 CODEC(BitPacking, ZSTD(3)),
    u32 UInt32 CODEC(BitPacking),
    i64 Int64 CODEC(BitPacking),
    u64 UInt64 CODEC(BitPacking, LZ4),
    d Date CODEC(BitPacking),
    d32 Date32 CODEC(BitPacking),
    t DateTime('UTC') CODEC(BitPacking),
    t64 DateTime64(3, 'UTC') CODEC(BitPacking, ZSTD),
    m32 Decimal32(2) CODEC(BitPacking),
    m64 Decimal64(4) CODEC(BitPacking),
    e Enum8('a' = -3, 'b' = 0, 'c' = 7) CODEC(BitPacking),
    ip IPv4 CODEC(BitPacking),
    c Int64 CODEC(BitPacking)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

CREATE TABLE bitpacking_plain AS bitpacking ENGINE = MergeTree ORDER BY tuple();

-- Rows below 300000 hold values close together in each chunk; the rest spread over all the bits of their type, which
-- a chunk stores as they are. `c` is a constant, so each of its chunks packs in 0 bits. The last block ends in the
-- middle of a chunk.
INSERT INTO bitpacking SELECT
    number % 1000,
    toInt8(if(number < 300000, -100 + number % 7, number % 256 - 128)),
    toUInt8(if(number < 300000, 200 + number % 5, number % 256)),
    toInt16(if(number < 300000, -30000 + number % 300, cityHash64(number) % 65536 - 32768)),
    toUInt16(if(number < 300000, 40000 + number % 300, cityHash64(number) % 65536)),
    toInt32(if(number < 300000, -5000000 + number % 100000, cityHash64(number))),
    toUInt32(if(number < 300000, 4000000000 + number % 100000, cityHash64(number))),
    toInt64(if(number < 300000, -(number % 1000003), cityHash64(number))),
    if(number < 300000, 18446744073709551000 + number % 600, cityHash64(number)),
    toDate('2020-01-01') + number % 2000,
    toDate32('1950-01-01') + number % 40000,
    toDateTime('2024-01-01 00:00:00', 'UTC') + number % 86400,
    toDateTime64('2024-01-01 00:00:00', 3, 'UTC') + (number % 86400) / 1000,
    toDecimal32((number % 100000) / 100, 2),
    toDecimal64(if(number < 300000, (number % 100000) / 100, toInt64(cityHash64(number)) / 10000), 4),
    CAST(['a', 'b', 'c'][1 + number % 3], 'Enum8(\'a\' = -3, \'b\' = 0, \'c\' = 7)'),
    toIPv4(if(number < 300000, 167772160 + number % 65536, toUInt32(cityHash64(number)))),
    42
FROM numbers(1000003);

INSERT INTO bitpacking_plain SELECT * FROM bitpacking;

SELECT count() FROM bitpacking;
SELECT (SELECT sum(cityHash64(*)) FROM bitpacking) = (SELECT sum(cityHash64(*)) FROM bitpacking_plain);
SELECT k, sum(i8), sum(u8), sum(i16), sum(u16), sum(i32), sum(u32), sum(i64), sum(u64), min(d), max(d32), max(t), min(t64), sum(m32), sum(m64), countIf(e = 'c'), max(ip), sum(c)
FROM bitpacking WHERE k < 3 GROUP BY k ORDER BY k;

-- A chunk of integers that lie close together packs in few bits - `k` takes 10 of its 32, `c` none - and one of
-- integers that take all their bits is stored as it is.
SELECT name, data_compressed_bytes < data_uncompressed_bytes / 3 FROM system.columns
WHERE database = currentDatabase() AND table = 'bitpacking' AND name IN ('k', 'c', 'u64') ORDER BY name;

-- A block of a few integers, and a block of one.
INSERT INTO bitpacking SELECT * FROM bitpacking_plain LIMIT 3;
INSERT INTO bitpacking SELECT * FROM bitpacking_plain LIMIT 1;
SELECT count() FROM bitpacking;

DROP TABLE bitpacking;
DROP TABLE bitpacking_plain;

-- The codec packs integers only.
CREATE TABLE bitpacking_bad (s String CODEC(BitPacking)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bitpacking_bad (f Float64 CODEC(BitPacking)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bitpacking_bad (m Decimal128(2) CODEC(BitPacking)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bitpacking_bad (x UInt64 CODEC(BitPacking(1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- It is experimental.
SET enable_bitpacking_codec = 0;
CREATE TABLE bitpacking_bad (x UInt64 CODEC(BitPacking)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
