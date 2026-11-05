-- A bucketed `Map` puts a key in the bucket of its hash, and `m[k]` with a constant `k` reads only the bucket of the
-- hash of `k`. For every class of key: the parts have 7 buckets, `m[k]` finds the keys and returns the default for the
-- other keys, and the whole `Map` reads back as it was written.

DROP VIEW IF EXISTS v_map_buckets_every_key_type_lookups;
DROP TABLE IF EXISTS t_map_buckets_every_key_type;
DROP TABLE IF EXISTS t_map_buckets_every_key_type_source;

CREATE TABLE t_map_buckets_every_key_type
(
    id UInt64,
    m_uint8 Map(UInt8, UInt64),
    m_uint16 Map(UInt16, UInt64),
    m_uint32 Map(UInt32, UInt64),
    m_uint64 Map(UInt64, UInt64),
    m_int8 Map(Int8, UInt64),
    m_int16 Map(Int16, UInt64),
    m_int32 Map(Int32, UInt64),
    m_int64 Map(Int64, UInt64),
    m_float32 Map(Float32, UInt64),
    m_float64 Map(Float64, UInt64),
    m_string Map(String, UInt64),
    m_fixed_string Map(FixedString(4), UInt64),
    m_uint128 Map(UInt128, UInt64),
    m_int256 Map(Int256, UInt64),
    m_uuid Map(UUID, UInt64),
    m_ipv4 Map(IPv4, UInt64),
    m_ipv6 Map(IPv6, UInt64),
    m_date Map(Date, UInt64),
    m_date32 Map(Date32, UInt64),
    m_datetime Map(DateTime('UTC'), UInt64),
    m_datetime64 Map(DateTime64(3, 'UTC'), UInt64),
    m_enum8 Map(Enum8('a' = -128, 'b' = -1, 'c' = 0, 'd' = 1, 'e' = 2, 'f' = 100, 'g' = 127), UInt64),
    m_decimal64 Map(Decimal64(4), UInt64),
    m_bool Map(Bool, UInt64),
    m_low_cardinality Map(LowCardinality(String), UInt64),
    m_array_uint8 Map(Array(UInt8), UInt64),
    m_array_float64 Map(Array(Float64), UInt64),
    m_tuple Map(Tuple(String, UInt64), UInt64),
    m_tuple_float64 Map(Tuple(Float64, UInt8), UInt64),
    m_variant Map(Variant(UInt64, String), UInt64),
    m_dynamic Map(Dynamic, UInt64),
    m_json Map(JSON, UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0, max_buckets_in_map = 7,
    serialization_info_version = 'with_types';

-- The key `j` (from 0 to 49) of each class is in the row `id` unless `(id + j) % 10 = 0`, with the value
-- `id * 100 + j + 1`. `Bool` has only the key 0 and `Enum8` the keys from 0 to 4.
CREATE TABLE t_map_buckets_every_key_type_source AS t_map_buckets_every_key_type ENGINE = Memory;

INSERT INTO t_map_buckets_every_key_type_source
SELECT
    id,
    mapFromArrays(arrayMap(j -> toUInt8(j * 4 + 3), js), vs) AS m_uint8,
    mapFromArrays(arrayMap(j -> toUInt16(j * 1201), js), vs) AS m_uint16,
    mapFromArrays(arrayMap(j -> toUInt32(j * 78125311), js), vs) AS m_uint32,
    mapFromArrays(arrayMap(j -> toUInt64(18446744073709551615 - j * 335544321234567), js), vs) AS m_uint64,
    mapFromArrays(arrayMap(j -> toInt8(toInt16(j * 4) - 110), js), vs) AS m_int8,
    mapFromArrays(arrayMap(j -> toInt16(toInt32(j * 1201) - 32768), js), vs) AS m_int16,
    mapFromArrays(arrayMap(j -> toInt32(toInt64(j * 78125311) - 2147483648), js), vs) AS m_int32,
    mapFromArrays(arrayMap(j -> 9000000000000000000 - toInt64(j) * 170000000000000000, js), vs) AS m_int64,
    mapFromArrays(arrayMap(j -> toFloat32(multiIf(j = 1, inf, j = 2, -inf, (j - 20.5) / 4)), js), vs) AS m_float32,
    mapFromArrays(arrayMap(j -> multiIf(j = 1, inf, j = 2, -inf, (j - 20.5) / 8), js), vs) AS m_float64,
    mapFromArrays(arrayMap(j -> concat('k', toString(j), repeat('x', j % 20)), js), vs) AS m_string,
    mapFromArrays(arrayMap(j -> toFixedString(toString(1000 + j * 37), 4), js), vs) AS m_fixed_string,
    mapFromArrays(arrayMap(j -> bitShiftLeft(toUInt128(j), 100) + j, js), vs) AS m_uint128,
    mapFromArrays(arrayMap(j -> (toInt256(j) - 27) * toInt256(repeat('9', 70)), js), vs) AS m_int256,
    mapFromArrays(arrayMap(j -> reinterpret(bitShiftLeft(toUInt128(j), 70) + j * 7919, 'UUID'), js), vs) AS m_uuid,
    mapFromArrays(arrayMap(j -> toIPv4(toUInt32(j * 78125311)), js), vs) AS m_ipv4,
    mapFromArrays(arrayMap(j -> toIPv6(bitShiftLeft(toUInt128(j + 1), 90) + j), js), vs) AS m_ipv6,
    mapFromArrays(arrayMap(j -> toDate('1970-01-01') + toUInt16(j * 1201), js), vs) AS m_date,
    mapFromArrays(arrayMap(j -> toDate32('1900-01-01') + toInt32(j * 2700), js), vs) AS m_date32,
    mapFromArrays(arrayMap(j -> toDateTime(toUInt32(j * 78125311), 'UTC'), js), vs) AS m_datetime,
    mapFromArrays(arrayMap(j -> fromUnixTimestamp64Milli(toInt64(j) * 123456789123 - 2208988800000, 'UTC'), js), vs) AS m_datetime64,
    mapFromArrays(arrayMap(j -> ['a', 'b', 'c', 'd', 'e', 'f', 'g'][j % 7 + 1], js5), vs5) AS m_enum8,
    mapFromArrays(arrayMap(j -> reinterpret(toInt64(j) * 123456789012 - 3000000000000, 'Decimal64(4)'), js), vs) AS m_decimal64,
    mapFromArrays(arrayMap(j -> CAST(j AS Bool), js1), vs1) AS m_bool,
    mapFromArrays(arrayMap(j -> concat('l', toString(j), repeat('y', j % 9)), js), vs) AS m_low_cardinality,
    mapFromArrays(arrayMap(j -> if(j = 0, [], arraySlice([toUInt8(j * 3), toUInt8(j), 7, 9], 1, 1 + j % 4)), js), vs) AS m_array_uint8,
    mapFromArrays(arrayMap(j -> [toFloat64(j) / 4 + 0.125, -toFloat64(j)], js), vs) AS m_array_float64,
    mapFromArrays(arrayMap(j -> (concat('t', toString(j)), j * 1000003), js), vs) AS m_tuple,
    mapFromArrays(arrayMap(j -> (toFloat64(j) / 2 - 10, toUInt8(j)), js), vs) AS m_tuple_float64,
    mapFromArrays(arrayMap(j -> if(j % 2 = 0, CAST(j AS Variant(UInt64, String)), CAST(concat('v', toString(j)) AS Variant(UInt64, String))), js), vs) AS m_variant,
    mapFromArrays(arrayMap(j -> multiIf(j % 3 = 0, CAST(toInt64(j) * 1000 - 7 AS Dynamic), j % 3 = 1, CAST(concat('d', toString(j)) AS Dynamic), CAST(j + 0.5 AS Dynamic)), js), vs) AS m_dynamic,
    mapFromArrays(arrayMap(j -> CAST(concat('{"a":', toString(j), ',"b":{"c":"s', toString(j), '"}}') AS JSON), js), vs) AS m_json
FROM
(
    SELECT
        number AS id,
        arrayFilter(j -> (id + j) % 10 != 0, range(toUInt64(50))) AS js,
        arrayMap(j -> id * 100 + j + 1, js) AS vs,
        arrayFilter(j -> j < 5, js) AS js5,
        arrayMap(j -> id * 100 + j + 1, js5) AS vs5,
        arrayFilter(j -> j < 1, js) AS js1,
        arrayMap(j -> id * 100 + j + 1, js1) AS vs1
    FROM numbers(100)
);

INSERT INTO t_map_buckets_every_key_type SELECT * FROM t_map_buckets_every_key_type_source;

-- Every bucket `b` of the `Map` column `m` has the substream `m.b.size0`.
SELECT column, groupUniqArray(arrayCount(s -> match(s, '^\\w+\\.\\d+\\.size0$'), substreams)) AS buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_map_buckets_every_key_type' AND active AND column != 'id'
GROUP BY column
ORDER BY column;

-- `m[k]` reads the bucket of `k` only for a constant `k`, so the view gets the key number `j` as a parameter.
SET optimize_functions_to_subcolumns = 1;

CREATE VIEW v_map_buckets_every_key_type_lookups AS
WITH {j:UInt64} AS j, if(j < 50 AND (id + j) % 10 != 0, id * 100 + j + 1, 0) AS expected
SELECT [
    ('UInt8', countIf(m_uint8[toUInt8(j * 4 + 3)] != expected)),
    ('UInt16', countIf(m_uint16[toUInt16(j * 1201)] != expected)),
    ('UInt32', countIf(m_uint32[toUInt32(j * 78125311)] != expected)),
    ('UInt64', countIf(m_uint64[toUInt64(18446744073709551615 - j * 335544321234567)] != expected)),
    ('Int8', countIf(m_int8[toInt8(toInt16(j * 4) - 110)] != expected)),
    ('Int16', countIf(m_int16[toInt16(toInt32(j * 1201) - 32768)] != expected)),
    ('Int32', countIf(m_int32[toInt32(toInt64(j * 78125311) - 2147483648)] != expected)),
    ('Int64', countIf(m_int64[9000000000000000000 - toInt64(j) * 170000000000000000] != expected)),
    ('Float32', countIf(m_float32[toFloat32(multiIf(j = 1, inf, j = 2, -inf, (j - 20.5) / 4))] != expected)),
    ('Float64', countIf(m_float64[multiIf(j = 1, inf, j = 2, -inf, (j - 20.5) / 8)] != expected)),
    ('String', countIf(m_string[concat('k', toString(j), repeat('x', j % 20))] != expected)),
    ('FixedString(4)', countIf(m_fixed_string[toFixedString(toString(1000 + j * 37), 4)] != expected)),
    ('UInt128', countIf(m_uint128[bitShiftLeft(toUInt128(j), 100) + j] != expected)),
    ('Int256', countIf(m_int256[(toInt256(j) - 27) * toInt256(repeat('9', 70))] != expected)),
    ('UUID', countIf(m_uuid[reinterpret(bitShiftLeft(toUInt128(j), 70) + j * 7919, 'UUID')] != expected)),
    ('IPv4', countIf(m_ipv4[toIPv4(toUInt32(j * 78125311))] != expected)),
    ('IPv6', countIf(m_ipv6[toIPv6(bitShiftLeft(toUInt128(j + 1), 90) + j)] != expected)),
    ('Date', countIf(m_date[toDate('1970-01-01') + toUInt16(j * 1201)] != expected)),
    ('Date32', countIf(m_date32[toDate32('1900-01-01') + toInt32(j * 2700)] != expected)),
    ('DateTime', countIf(m_datetime[toDateTime(toUInt32(j * 78125311), 'UTC')] != expected)),
    ('DateTime64(3)', countIf(m_datetime64[fromUnixTimestamp64Milli(toInt64(j) * 123456789123 - 2208988800000, 'UTC')] != expected)),
    ('Enum8', countIf(j < 7 AND m_enum8[['a', 'b', 'c', 'd', 'e', 'f', 'g'][j % 7 + 1]] != if(j < 5, expected, 0))),
    ('Decimal64(4)', countIf(m_decimal64[reinterpret(toInt64(j) * 123456789012 - 3000000000000, 'Decimal64(4)')] != expected)),
    ('Bool', countIf(j < 2 AND m_bool[CAST(j AS Bool)] != if(j < 1, expected, 0))),
    ('LowCardinality(String)', countIf(m_low_cardinality[concat('l', toString(j), repeat('y', j % 9))] != expected)),
    ('Array(UInt8)', countIf(m_array_uint8[if(j = 0, [], arraySlice([toUInt8(j * 3), toUInt8(j), 7, 9], 1, 1 + j % 4))] != expected)),
    ('Array(Float64)', countIf(m_array_float64[[toFloat64(j) / 4 + 0.125, -toFloat64(j)]] != expected)),
    ('Tuple(String, UInt64)', countIf(m_tuple[(concat('t', toString(j)), j * 1000003)] != expected)),
    ('Tuple(Float64, UInt8)', countIf(m_tuple_float64[(toFloat64(j) / 2 - 10, toUInt8(j))] != expected)),
    ('Variant(UInt64, String)', countIf(m_variant[if(j % 2 = 0, CAST(j AS Variant(UInt64, String)), CAST(concat('v', toString(j)) AS Variant(UInt64, String)))] != expected)),
    ('Dynamic', countIf(m_dynamic[multiIf(j % 3 = 0, CAST(toInt64(j) * 1000 - 7 AS Dynamic), j % 3 = 1, CAST(concat('d', toString(j)) AS Dynamic), CAST(j + 0.5 AS Dynamic))] != expected)),
    ('JSON', countIf(m_json[CAST(concat('{"a":', toString(j), ',"b":{"c":"s', toString(j), '"}}') AS JSON)] != expected))
] AS result
FROM t_map_buckets_every_key_type;

-- Mismatches per class for the keys from 0 to 9, which have the special values of every class and are absent from some
-- rows, and for the key 50, which is in no row.
SELECT r.1 AS key_type, sum(r.2) AS mismatches
FROM
(
    SELECT * FROM v_map_buckets_every_key_type_lookups(j = 0) UNION ALL SELECT * FROM v_map_buckets_every_key_type_lookups(j = 1) UNION ALL
    SELECT * FROM v_map_buckets_every_key_type_lookups(j = 2) UNION ALL SELECT * FROM v_map_buckets_every_key_type_lookups(j = 3) UNION ALL
    SELECT * FROM v_map_buckets_every_key_type_lookups(j = 4) UNION ALL SELECT * FROM v_map_buckets_every_key_type_lookups(j = 5) UNION ALL
    SELECT * FROM v_map_buckets_every_key_type_lookups(j = 6) UNION ALL SELECT * FROM v_map_buckets_every_key_type_lookups(j = 7) UNION ALL
    SELECT * FROM v_map_buckets_every_key_type_lookups(j = 8) UNION ALL SELECT * FROM v_map_buckets_every_key_type_lookups(j = 9) UNION ALL
    SELECT * FROM v_map_buckets_every_key_type_lookups(j = 50)
)
ARRAY JOIN result AS r, arrayEnumerate(result) AS n
GROUP BY n, key_type
ORDER BY n;

-- The rows have keys, so the mismatches above are not vacuous.
SELECT count(), sum(length(m_uint8)), sum(length(m_json)) FROM t_map_buckets_every_key_type;

-- The whole `Map` of every class reads back as written.
SELECT count() FROM
(
    SELECT COLUMNS('^m_') APPLY(x -> groupBitXor(cityHash64(id, x))) FROM t_map_buckets_every_key_type
    UNION DISTINCT
    SELECT COLUMNS('^m_') APPLY(x -> groupBitXor(cityHash64(id, x))) FROM t_map_buckets_every_key_type_source
);

DROP VIEW v_map_buckets_every_key_type_lookups;
DROP TABLE t_map_buckets_every_key_type;
DROP TABLE t_map_buckets_every_key_type_source;
