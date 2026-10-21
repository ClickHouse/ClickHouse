-- The `obfuscate` table function supports `LowCardinality`, `Enum`, `Date32`, `DateTime64`, `Decimal`,
-- wide integers, `IPv4`, `IPv6`, `BFloat16`, `Tuple`, `Map` and `Nullable(Nothing)`.
-- A table like `uk_price_paid` (with `LowCardinality(String)` and `Enum8` columns) can be obfuscated.

SET obfuscate_seed = 'seed';
SET max_threads = 1;

DROP TABLE IF EXISTS t_obfuscate_types;
CREATE TABLE t_obfuscate_types
(
    id UInt64,
    lc LowCardinality(String),
    lcn LowCardinality(Nullable(String)),
    e8 Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4, 'e' = 5),
    e16 Enum16('x' = -1000, 'y' = 0, 'z' = 1000),
    d32 Date32,
    dt64 DateTime64(3, 'UTC'),
    dt64_0 DateTime64(0, 'Asia/Kolkata'),
    dec32 Decimal32(2),
    dec128 Decimal128(10),
    u128 UInt128,
    i256 Int256,
    ip4 IPv4,
    ip6 IPv6,
    tup Tuple(a UInt32, b String),
    m Map(String, UInt64),
    bf BFloat16
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_obfuscate_types SELECT
    number,
    'value_' || toString(number % 10),
    if(number % 3 = 0, NULL, 'v' || toString(number % 7)),
    1 + number % 5,
    ['x', 'y', 'z'][1 + number % 3],
    toDate32('1950-01-01') + number * 100,
    if(number % 2 = 0, toDateTime64('1960-01-01 12:34:56.789', 3, 'UTC'), toDateTime64('2020-03-29 00:00:00.001', 3, 'UTC')) + toIntervalMillisecond(number * 3607123),
    toDateTime64('2020-01-01 00:00:00', 0, 'Asia/Kolkata') + toIntervalSecond(number * 12345),
    toDecimal32((toInt64(number) - 50) * 1.37, 2),
    toDecimal128((toInt64(number) - 50) * 123456789.123, 10),
    toUInt128(number) * toUInt128('12345678901234567890123'),
    if(number = 0, toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), (toInt256(number) - 50) * toInt256('123456789012345678901234567890123')),
    toIPv4(167772160 + number * 16777619),
    toIPv6('2001:db8::' || toString(number)),
    (number, toString(number)),
    mapFromArrays(arrayMap(i -> 'k' || toString(i), range(number % 4)), range(number % 4)),
    toBFloat16(number / 7)
FROM numbers(100);

-- The types are kept.
DESCRIBE (SELECT * FROM obfuscate(SELECT * EXCEPT id FROM t_obfuscate_types));

-- Several passes over the source.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * EXCEPT id FROM t_obfuscate_types) LIMIT 1000);

-- The first pass transforms the source rows in order, so the source and the obfuscated rows are compared one by one.
DROP VIEW IF EXISTS v_src;
DROP VIEW IF EXISTS v_obf;
CREATE VIEW v_src AS SELECT * EXCEPT id FROM t_obfuscate_types ORDER BY id;
CREATE VIEW v_obf AS SELECT * FROM obfuscate(SELECT * EXCEPT id FROM t_obfuscate_types ORDER BY id) LIMIT 100;

SELECT 'LowCardinality', uniqExact(lc) BETWEEN 1 AND 10, countIf(lc != '') > 0 FROM v_obf;
SELECT 'LowCardinality(Nullable) keeps the null map', (SELECT groupArray(isNull(lcn)) FROM v_src) = (SELECT groupArray(isNull(lcn)) FROM v_obf);

-- An `Enum` is mapped by a permutation of its values: each source value always becomes the same value,
-- and different source values become different values.
SELECT 'Enum',
    length(arrayDistinct(arrayZip((SELECT groupArray(e8) FROM v_src), (SELECT groupArray(e8) FROM v_obf)))),
    length(arrayDistinct((SELECT groupArray(e8) FROM v_obf))),
    length(arrayDistinct(arrayZip((SELECT groupArray(e16) FROM v_src), (SELECT groupArray(e16) FROM v_obf)))),
    length(arrayDistinct((SELECT groupArray(e16) FROM v_obf)));

SELECT 'Date32 is kept', (SELECT groupArray(d32) FROM v_src) = (SELECT groupArray(d32) FROM v_obf);

SELECT 'DateTime64 keeps the date',
    (SELECT groupArray(toDate(dt64)) FROM v_src) = (SELECT groupArray(toDate(dt64)) FROM v_obf),
    (SELECT groupArray(toDate(dt64_0)) FROM v_src) = (SELECT groupArray(toDate(dt64_0)) FROM v_obf),
    (SELECT groupArray(dt64) FROM v_src) != (SELECT groupArray(dt64) FROM v_obf),
    (SELECT groupArray(dt64_0) FROM v_src) != (SELECT groupArray(dt64_0) FROM v_obf);

-- The numeric types keep the sign and the magnitude up to a power of two, and the mapping is injective.
SELECT 'Decimal',
    (SELECT groupArray((sign(dec32), floor(log2(abs(toInt32(dec32 * 100)))))) FROM v_src) = (SELECT groupArray((sign(dec32), floor(log2(abs(toInt32(dec32 * 100)))))) FROM v_obf),
    (SELECT groupArray(sign(dec128)) FROM v_src) = (SELECT groupArray(sign(dec128)) FROM v_obf),
    (SELECT uniqExact(dec128) FROM v_obf) = (SELECT uniqExact(dec128) FROM v_src),
    (SELECT groupArray(dec32) FROM v_src) != (SELECT groupArray(dec32) FROM v_obf);

SELECT 'wide integers',
    (SELECT groupArray(sign(i256)) FROM v_src) = (SELECT groupArray(sign(i256)) FROM v_obf),
    (SELECT uniqExact(u128) FROM v_obf) = (SELECT uniqExact(u128) FROM v_src),
    (SELECT uniqExact(i256) FROM v_obf) = (SELECT uniqExact(i256) FROM v_src),
    (SELECT min(i256) FROM v_obf) = (SELECT min(i256) FROM v_src),
    (SELECT groupArray(u128) FROM v_src) != (SELECT groupArray(u128) FROM v_obf);

SELECT 'IP',
    (SELECT groupArray(floor(log2(toUInt32(ip4)))) FROM v_src) = (SELECT groupArray(floor(log2(toUInt32(ip4)))) FROM v_obf),
    (SELECT uniqExact(ip4) FROM v_obf) = 100,
    (SELECT uniqExact(ip6) FROM v_obf) = 100,
    (SELECT groupArray(ip4) FROM v_src) != (SELECT groupArray(ip4) FROM v_obf);

SELECT 'Tuple', (SELECT groupArray(floor(log2(tup.a))) FROM v_src) = (SELECT groupArray(floor(log2(tup.a))) FROM v_obf);
SELECT 'Map keeps the sizes', (SELECT groupArray(length(m)) FROM v_src) = (SELECT groupArray(length(m)) FROM v_obf);
SELECT 'BFloat16', count(), countIf(bf >= 0) FROM v_obf;

SELECT * FROM obfuscate(SELECT NULL AS x FROM numbers(3)) LIMIT 3;

DROP VIEW v_obf;
DROP VIEW v_src;
DROP TABLE t_obfuscate_types;
