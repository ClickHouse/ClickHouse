-- The `GDeflate` codec: values read back as they were written, for each kind of column, with blocks of several chunks
-- and a last chunk shorter than the others; and errors.

DROP TABLE IF EXISTS t_gdeflate;
DROP TABLE IF EXISTS t_gdeflate_plain;
DROP TABLE IF EXISTS t_gdeflate_levels;
DROP TABLE IF EXISTS t_gdeflate_page;

-- `GDeflate` is experimental: without the setting it is rejected.
SET enable_gdeflate_codec = 0;
CREATE TABLE t_gdeflate (x UInt64 CODEC(GDeflate)) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

SET enable_gdeflate_codec = 1;

CREATE TABLE t_gdeflate (x UInt64 CODEC(GDeflate(0))) ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_gdeflate (x UInt64 CODEC(GDeflate(13))) ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_gdeflate (x UInt64 CODEC(GDeflate('6'))) ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_gdeflate (x UInt64 CODEC(GDeflate(1, 2))) ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

CREATE TABLE t_gdeflate
(
    k UInt64,
    i8 Int8 CODEC(GDeflate),
    u16 UInt16 CODEC(GDeflate),
    i32 Int32 CODEC(GDeflate),
    i64 Int64 CODEC(GDeflate),
    u64 UInt64 CODEC(GDeflate),
    d Date CODEC(GDeflate),
    d32 Date32 CODEC(GDeflate),
    t DateTime('UTC') CODEC(GDeflate),
    t64 DateTime64(3, 'UTC') CODEC(GDeflate),
    m Decimal64(2) CODEC(GDeflate),
    f Float64 CODEC(GDeflate),
    s String CODEC(GDeflate),
    n Nullable(UInt32) CODEC(GDeflate),
    a Array(UInt16) CODEC(GDeflate),
    lc LowCardinality(String) CODEC(GDeflate),
    dz Int64 CODEC(Delta, GDeflate)
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

CREATE TABLE t_gdeflate_plain
(
    k UInt64, i8 Int8, u16 UInt16, i32 Int32, i64 Int64, u64 UInt64, d Date, d32 Date32, t DateTime('UTC'),
    t64 DateTime64(3, 'UTC'), m Decimal64(2), f Float64, s String, n Nullable(UInt32), a Array(UInt16),
    lc LowCardinality(String), dz Int64
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_gdeflate SELECT
    number,
    toInt8(if(number < 100000, -100 + number % 7, number % 256 - 128)),
    toUInt16(if(number < 100000, 40000 + number % 300, cityHash64(number) % 65536)),
    toInt32(if(number < 100000, -5000000 + number % 100000, cityHash64(number))),
    toInt64(if(number < 100000, -(number % 1000003), cityHash64(number))),
    if(number < 100000, 18446744073709551000 + number % 600, cityHash64(number)),
    toDate('2020-01-01') + number % 2000,
    toDate32('1950-01-01') + number % 30000,
    toDateTime('2024-01-01 00:00:00', 'UTC') + number % 86400,
    toDateTime64('2024-01-01 00:00:00', 3, 'UTC') + toIntervalMillisecond(number * 7),
    toDecimal64((number % 100000) / 100, 2),
    number / 7,
    if(number % 5 = 0, '', concat('value_', toString(number % 10007))),
    if(number % 3 = 0, NULL, toUInt32(number % 5000)),
    range(number % 5),
    toString(number % 17),
    toInt64(number) * 3
FROM numbers(300007);

INSERT INTO t_gdeflate_plain SELECT * FROM t_gdeflate;

SELECT 'roundtrip', (SELECT sum(cityHash64(*)) FROM t_gdeflate) = (SELECT sum(cityHash64(*)) FROM t_gdeflate_plain);
SELECT 'rows', count(), sum(i8), sum(u16), sum(i32), sum(i64), sum(u64), min(d32), max(t64), sum(m), sum(dz) FROM t_gdeflate;

-- Each level reads back what it wrote.
CREATE TABLE t_gdeflate_levels
(
    k UInt64,
    l1 UInt32 CODEC(GDeflate(1)),
    l6 UInt32 CODEC(GDeflate(6)),
    l9 UInt32 CODEC(GDeflate(9)),
    l12 UInt32 CODEC(GDeflate(12)),
    s1 String CODEC(GDeflate(1)),
    s12 String CODEC(GDeflate(12))
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_gdeflate_levels SELECT number, number % 1000, number % 1000, number % 1000, number % 1000, toString(number % 977), toString(number % 977)
FROM numbers(100000);
SELECT 'levels', count(), sum(l1), sum(l1) = sum(l6) AND sum(l6) = sum(l9) AND sum(l9) = sum(l12), sum(cityHash64(s1)) = sum(cityHash64(s12))
FROM t_gdeflate_levels;

-- A page that compresses to exactly the end of libdeflate's own bound.
CREATE TABLE t_gdeflate_page (k UInt32, c UInt8 CODEC(GDeflate))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 65536,
    min_columns_to_activate_adaptive_write_buffer = 1000, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_gdeflate_page SELECT number, toUInt8(if(number < 117, number % 7, cityHash64(number) % 256)) FROM numbers(65536)
SETTINGS max_block_size = 65536;
SELECT 'page', count(), sum(cityHash64(k, c)) = (
    SELECT sum(cityHash64(toUInt32(number), toUInt8(if(number < 117, number % 7, cityHash64(number) % 256)))) FROM numbers(65536))
FROM t_gdeflate_page;

DROP TABLE t_gdeflate;
DROP TABLE t_gdeflate_plain;
DROP TABLE t_gdeflate_levels;
DROP TABLE t_gdeflate_page;
