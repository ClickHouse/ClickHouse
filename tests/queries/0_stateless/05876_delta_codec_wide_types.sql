-- `Delta` codec on 16- and 32-byte types. Only `Delta` is covered here: the other specialized codecs
-- (`DoubleDelta`, `Gorilla`, `T64`) are still limited to at most 8 bytes.
-- https://github.com/ClickHouse/ClickHouse/issues/61444
--
-- The use case is slowly changing, large and precise values such as balances: the values are big,
-- but consecutive values differ only by a small amount, which is exactly what delta encoding exploits.

SET optimize_trivial_insert_select = 0;

DROP TABLE IF EXISTS wide_delta;

CREATE TABLE wide_delta
(
    id UInt64,

    u128_lz4 UInt128 CODEC(LZ4),
    u128_delta UInt128 CODEC(Delta, LZ4),

    i128_lz4 Int128 CODEC(LZ4),
    i128_delta Int128 CODEC(Delta, LZ4),

    u256_lz4 UInt256 CODEC(LZ4),
    u256_delta UInt256 CODEC(Delta, LZ4),

    i256_lz4 Int256 CODEC(LZ4),
    i256_delta Int256 CODEC(Delta, LZ4),

    d128_lz4 Decimal128(18) CODEC(LZ4),
    d128_delta Decimal128(18) CODEC(Delta, LZ4),

    d256_lz4 Decimal256(40) CODEC(LZ4),
    d256_delta Decimal256(40) CODEC(Delta, LZ4),

    -- Random values over the full width of the type: every delta wraps around and carries across the 64-bit limbs.
    rand128_delta UInt128 CODEC(Delta, LZ4),
    rand256_delta UInt256 CODEC(Delta, LZ4),

    -- Not a useful combination, but `Delta` accepts every fixed-size type of size 1, 2, 4, 8, 16 or 32 bytes.
    uuid_delta UUID CODEC(Delta, LZ4),
    ipv6_delta IPv6 CODEC(Delta, LZ4)
)
ENGINE = MergeTree
ORDER BY id
-- The sizes of granules and compressed blocks are pinned, because the test compares compressed sizes:
-- every compressed block stores its first value as is, so tiny blocks would defeat delta encoding.
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    index_granularity = 8192, index_granularity_bytes = 10485760,
    min_compress_block_size = 65536, max_compress_block_size = 1048576;

-- A balance-like series: a large base value plus a slowly growing amount with a bit of noise.
-- Signed columns hold negative values, and the values need the full width of the type
-- (the base exceeds 2^64 for 128-bit types and 2^128 for 256-bit types).
INSERT INTO wide_delta
SELECT
    number AS id,

    toUInt128('100000000000000000000000000000') + number * 1000003 + number % 7 AS u128,
    u128,

    toInt128('-50000000000000000000000000000') + toInt128(number) * 1000003 - number % 7 AS i128,
    i128,

    toUInt256('1000000000000000000000000000000000000000000000000000') + number * 1000003 + number % 7 AS u256,
    u256,

    toInt256('-500000000000000000000000000000000000000000000000000') + toInt256(number) * 1000003 - number % 7 AS i256,
    i256,

    toDecimal128('12345678901234567890.123456789012345678', 18) + toDecimal128(number, 18) / 1000 AS d128,
    d128,

    toDecimal256('123456789012345678901234567890.1234567890123456789012345678901234567890', 40) + toDecimal256(number, 40) / 1000 AS d256,
    d256,

    reinterpretAsUInt128(sipHash128(number)),
    reinterpretAsUInt256(concat(sipHash128(number, 1), sipHash128(number, 2))),

    reinterpretAsUUID(sipHash128(number)),
    toIPv6(IPv4NumToString(toUInt32(number)))
FROM numbers(100000);

OPTIMIZE TABLE wide_delta FINAL;

SELECT 'Round trip';
SELECT
    countIf(u128_delta != u128_lz4),
    countIf(i128_delta != i128_lz4),
    countIf(u256_delta != u256_lz4),
    countIf(i256_delta != i256_lz4),
    countIf(d128_delta != d128_lz4),
    countIf(d256_delta != d256_lz4)
FROM wide_delta;

-- Values must also survive decoding exactly, not just compare equal to each other.
SELECT min(u128_delta), max(u128_delta), min(i128_delta), max(i256_delta), max(d256_delta)
FROM wide_delta;

-- Columns without a plain copy are compared to what was inserted.
SELECT
    countIf(rand128_delta != reinterpretAsUInt128(sipHash128(id))),
    countIf(rand256_delta != reinterpretAsUInt256(concat(sipHash128(id, 1), sipHash128(id, 2)))),
    countIf(uuid_delta != reinterpretAsUUID(sipHash128(id))),
    countIf(ipv6_delta != toIPv6(IPv4NumToString(toUInt32(id))))
FROM wide_delta;

-- The random column really uses the upper limbs.
SELECT countIf(rand256_delta > toUInt256('340282366920938463463374607431768211455')) > 99000 FROM wide_delta;

-- `Delta` must actually help on this data: compared to plain `LZ4`, the delta-encoded columns
-- must be at least 4 times smaller. The real ratio is much higher, the margin is only for stability.
SELECT 'Compression';
SELECT type, delta_bytes * 4 < lz4_bytes AS delta_helps
FROM
(
    SELECT
        replaceRegexpOne(name, '_(lz4|delta)$', '') AS type,
        sumIf(data_compressed_bytes, endsWith(name, '_lz4')) AS lz4_bytes,
        sumIf(data_compressed_bytes, endsWith(name, '_delta')) AS delta_bytes
    FROM system.columns
    WHERE database = currentDatabase() AND table = 'wide_delta' AND match(name, '^[uid](128|256)_(lz4|delta)$')
    GROUP BY type
)
ORDER BY type;

-- An explicit width argument now also accepts 16 and 32.
SELECT 'Explicit width';
DROP TABLE IF EXISTS wide_delta_explicit;
CREATE TABLE wide_delta_explicit (n UInt256 CODEC(Delta(32), LZ4), m Int128 CODEC(Delta(16), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO wide_delta_explicit SELECT toUInt256('1000000000000000000000000000000000000000') + number, -toInt128('100000000000000000000000') - number FROM numbers(1000);
SELECT count(), min(n), max(n), min(m), max(m) FROM wide_delta_explicit;

DROP TABLE wide_delta_explicit;

-- The extreme values of the types next to each other: the deltas wrap around and borrow through all limbs.
-- Also the types that only wrap a wide integer (`Nullable`, `Array`, `Tuple`).
SELECT 'Extreme values';
DROP TABLE IF EXISTS wide_delta_extremes;
CREATE TABLE wide_delta_extremes
(
    id UInt64,

    u128_none UInt128 CODEC(NONE),
    u128_delta UInt128 CODEC(Delta, LZ4),

    i128_none Int128 CODEC(NONE),
    i128_delta Int128 CODEC(Delta, LZ4),

    u256_none UInt256 CODEC(NONE),
    u256_delta UInt256 CODEC(Delta, LZ4),

    i256_none Int256 CODEC(NONE),
    i256_delta Int256 CODEC(Delta, LZ4),

    nullable_none Nullable(Int256) CODEC(NONE),
    nullable_delta Nullable(Int256) CODEC(Delta, LZ4),

    array_none Array(UInt128) CODEC(NONE),
    array_delta Array(UInt128) CODEC(Delta, LZ4),

    tuple_none Tuple(Int128, UInt256) CODEC(NONE),
    tuple_delta Tuple(Int128, UInt256) CODEC(Delta, LZ4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO wide_delta_extremes
SELECT
    number AS id,

    [toUInt128(0), toUInt128('340282366920938463463374607431768211455'), toUInt128(1), toUInt128('18446744073709551616')][number % 4 + 1] AS u128,
    u128,

    [toInt128('-170141183460469231731687303715884105728'), toInt128('170141183460469231731687303715884105727'), toInt128(0), toInt128(-1)][number % 4 + 1] AS i128,
    i128,

    [toUInt256(0), toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), toUInt256(1), toUInt256('340282366920938463463374607431768211456')][number % 4 + 1] AS u256,
    u256,

    [toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'), toInt256(0), toInt256(-1)][number % 4 + 1] AS i256,
    i256,

    if(number % 3 = 0, NULL, i256) AS nullable,
    nullable,

    arrayMap(x -> u128 + x, range(number % 5)) AS array,
    array,

    (i128, u256) AS tuple,
    tuple
FROM numbers(10000);

OPTIMIZE TABLE wide_delta_extremes FINAL;

SELECT
    countIf(u128_delta != u128_none),
    countIf(i128_delta != i128_none),
    countIf(u256_delta != u256_none),
    countIf(i256_delta != i256_none),
    countIf(NOT (nullable_delta <=> nullable_none)),
    countIf(array_delta != array_none),
    countIf(tuple_delta != tuple_none)
FROM wide_delta_extremes;

SELECT min(u128_delta), max(u128_delta) FROM wide_delta_extremes;
SELECT min(i128_delta), max(i128_delta) FROM wide_delta_extremes;
SELECT min(u256_delta), max(u256_delta) FROM wide_delta_extremes;
SELECT min(i256_delta), max(i256_delta) FROM wide_delta_extremes;
SELECT countIf(nullable_delta IS NULL), min(nullable_delta), max(nullable_delta), sum(length(array_delta)), min(tuple_delta.1), max(tuple_delta.2) FROM wide_delta_extremes;

DROP TABLE wide_delta_extremes;

-- The explicit width does not have to match the type. With an odd number of 8-byte values and a width of 16
-- the data is not a multiple of the width, and the remaining bytes are stored as is.
SELECT 'Unaligned tail';
DROP TABLE IF EXISTS wide_delta_tail;
CREATE TABLE wide_delta_tail (x UInt64 CODEC(Delta(16), LZ4), y UInt128 CODEC(Delta(32), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO wide_delta_tail SELECT number * 3, toUInt128('100000000000000000000000000000') + number FROM numbers(1001);
SELECT count(), sum(x), min(y), max(y) FROM wide_delta_tail;
DROP TABLE wide_delta_tail;
DROP TABLE wide_delta;
