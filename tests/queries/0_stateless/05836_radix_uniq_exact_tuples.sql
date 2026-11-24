-- `uniqExact` of a tuple whose elements have a fixed size of 1, 2, 4, 8 or 16 bytes and fit into 16 bytes counts the values
-- of their bytes with radix partitioning, and must count as many distinct values as without it. Other tuples are hashed.

SET max_threads = 4, collect_hash_table_stats_during_aggregation = 0, enable_parallel_replicas = 0;

SELECT 'Tuple(UInt8, UInt16)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toUInt8(number % 200), toUInt16(intDiv(number % 200000, 200))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toUInt8(number % 200), toUInt16(intDiv(number % 200000, 200))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(Int32, Date)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toInt32(number % 1000) - 500, toDate(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toInt32(number % 1000) - 500, toDate(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(Int32, Int64)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toInt32(-(number % 7)), -toInt64(number % 200000)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toInt32(-(number % 7)), -toInt64(number % 200000)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(UInt64, UInt32, UInt16, UInt8, UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (number % 200000, toUInt32(number % 3), toUInt16(number % 5), toUInt8(number % 2), toUInt8(1)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (number % 200000, toUInt32(number % 3), toUInt16(number % 5), toUInt8(number % 2), toUInt8(1)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(UInt128)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT tuple(toUInt128(number % 200000) * 1000003) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT tuple(toUInt128(number % 200000) * 1000003) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- Floats are compared the same way in both cases: -0.0 and 0.0 are one value, while NaNs with other bits are different values.
SELECT 'Tuple(Float64, UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (multiIf(number % 4 = 0, -0., number % 4 = 1, 0., number % 4 = 2, nan, reinterpretAsFloat64(reinterpretAsUInt64(nan) + 1)), toUInt8(number % 3)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (multiIf(number % 4 = 0, -0., number % 4 = 1, 0., number % 4 = 2, nan, reinterpretAsFloat64(reinterpretAsUInt64(nan) + 1)), toUInt8(number % 3)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(FixedString(2), Bool, Enum8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toFixedString(char(65 + number % 26, 65 + intDiv(number, 26) % 26), 2), number % 2 = 1, CAST(number % 3 AS Enum8('a' = 0, 'b' = 1, 'c' = 2))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toFixedString(char(65 + number % 26, 65 + intDiv(number, 26) % 26), 2), number % 2 = 1, CAST(number % 3 AS Enum8('a' = 0, 'b' = 1, 'c' = 2))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- Not packed: 17 bytes.
SELECT 'Tuple(UInt64, UInt64, UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (number % 200000, number % 3, toUInt8(number % 2)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (number % 200000, number % 3, toUInt8(number % 2)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- A `Nullable` element is packed as its null flag and its value, zero for NULL, whatever the nested column holds under it.
SELECT 'Tuple(Nullable(Int32), Int64)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (bitXor(if(number % 5 = 0, NULL, toInt32(toInt32(number % 1000) - 500)), toInt32(number % 3)), toInt64(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (bitXor(if(number % 5 = 0, NULL, toInt32(toInt32(number % 1000) - 500)), toInt32(number % 3)), toInt64(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(Nullable(Float64), UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (multiIf(number % 5 = 0, NULL, number % 5 = 1, -0., number % 5 = 2, 0., number % 5 = 3, nan, 1.5), toUInt8(number % 3)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (multiIf(number % 5 = 0, NULL, number % 5 = 1, -0., number % 5 = 2, 0., number % 5 = 3, nan, 1.5), toUInt8(number % 3)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(Nullable(UInt64), Nullable(UInt64))', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (if(number % 3 = 0, NULL, number % 200000), if(number % 7 = 0, NULL, number % 5)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (if(number % 3 = 0, NULL, number % 200000), if(number % 7 = 0, NULL, number % 5)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(Nullable(UInt32), UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (if(number % 10 = 0, NULL, toUInt32(number % 200000)), toUInt8(number % 2)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (if(number % 10 = 0, NULL, toUInt32(number % 200000)), toUInt8(number % 2)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- Fixed-size values and strings, `Nullable` or not, are hashed by their bytes. NULL, 0 and the empty string are three values.
SELECT 'Tuple(Int32, String)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toInt32(number % 1000) - 500, toString(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toInt32(number % 1000) - 500, toString(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(String, String)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (repeat('a', number % 20), toString(number % 200000)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (repeat('a', number % 20), toString(number % 200000)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(Nullable(UInt8), Nullable(String))', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (if(number % 3 = 0, NULL, toUInt8(number % 2)), if(number % 5 = 0, NULL, if(number % 7 = 0, '', toString(number % 200000)))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (if(number % 3 = 0, NULL, toUInt8(number % 2)), if(number % 5 = 0, NULL, if(number % 7 = 0, '', toString(number % 200000)))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'Tuple(String, Nullable(FixedString(3)), Float32)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toString(number % 100), if(number % 4 = 0, NULL, toFixedString(toString(number % 200000 % 900 + 100), 3)), toFloat32(number % 3) / 2) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toString(number % 100), if(number % 4 = 0, NULL, toFixedString(toString(number % 200000 % 900 + 100), 3)), toFloat32(number % 3) / 2) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- Not packed: a `LowCardinality` element holds indexes into its dictionary, not its values.
SELECT 'Tuple(LowCardinality(UInt32), UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toLowCardinality(toUInt32(number % 100000)), toUInt8(number % 3)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, allow_suspicious_low_cardinality_types = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toLowCardinality(toUInt32(number % 100000)), toUInt8(number % 3)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0, allow_suspicious_low_cardinality_types = 1) AS count_ordinary
);

SELECT 'Tuple(LowCardinality(Nullable(UInt16)), Int64)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toLowCardinality(if(number % 5 = 0, NULL, toUInt16(number % 1000))), toInt64(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, allow_suspicious_low_cardinality_types = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toLowCardinality(if(number % 5 = 0, NULL, toUInt16(number % 1000))), toInt64(intDiv(number % 200000, 1000))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0, allow_suspicious_low_cardinality_types = 1) AS count_ordinary
);

-- Runs of equal tuples longer than the 256 rows hashed at a time, also of NULL and not NULL values.
SELECT 'runs of Tuple(UInt64, UInt64, UInt8)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (intDiv(number, 300), intDiv(number, 7000), toUInt8(intDiv(number, 1000) % 2)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (intDiv(number, 300), intDiv(number, 7000), toUInt8(intDiv(number, 1000) % 2)) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'runs of Tuple(String, Nullable(UInt32))', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toString(intDiv(number, 300)), if(intDiv(number, 600) % 3 = 0, NULL, toUInt32(intDiv(number, 1000)))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toString(intDiv(number, 300)), if(intDiv(number, 600) % 3 = 0, NULL, toUInt32(intDiv(number, 1000)))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- The rows of a run are compared the same way: -0.0 and 0.0 are one value, while NaNs with other bits are different values.
SELECT 'runs of Tuple(String, Float64)', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (toString(intDiv(number, 1000)), multiIf(intDiv(number, 100) % 4 = 0, -0., intDiv(number, 100) % 4 = 1, 0., intDiv(number, 100) % 4 = 2, nan, reinterpretAsFloat64(reinterpretAsUInt64(nan) + 1))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (toString(intDiv(number, 1000)), multiIf(intDiv(number, 100) % 4 = 0, -0., intDiv(number, 100) % 4 = 1, 0., intDiv(number, 100) % 4 = 2, nan, reinterpretAsFloat64(reinterpretAsUInt64(nan) + 1))) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

-- A constant element, and spilled keys.
SELECT 'constant element', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact(x) FROM (SELECT (number % 200000, 7) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact(x) FROM (SELECT (number % 200000, 7) AS x FROM numbers_mt(1000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'spill', uniqExact(x) FROM (SELECT (toInt32(-(number % 7)), -toInt64(number % 200000)) AS x FROM numbers_mt(1000000))
SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_bytes_before_external_group_by = 1000000, max_bytes_ratio_before_external_group_by = 0;

SELECT 'no rows', uniqExact(x) FROM (SELECT (toUInt8(number), toUInt16(number)) AS x FROM numbers_mt(1000000) WHERE number > 1000000)
SETTINGS optimize_uniq_exact_radix_partitioning = 1;

-- Sparse elements of a table.
DROP TABLE IF EXISTS t_radix_packed_tuple;
CREATE TABLE t_radix_packed_tuple (a UInt32, b UInt16) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, index_granularity = 8192;
INSERT INTO t_radix_packed_tuple SELECT number % 200000, if(number % 100 = 0, toUInt16(number % 7), 0) FROM numbers(1000000);

SELECT 'sparse elements', count_radix, count_radix = count_ordinary FROM
(
    SELECT
        (SELECT uniqExact((a, b)) FROM t_radix_packed_tuple SETTINGS optimize_uniq_exact_radix_partitioning = 1) AS count_radix,
        (SELECT uniqExact((a, b)) FROM t_radix_packed_tuple SETTINGS optimize_uniq_exact_radix_partitioning = 0) AS count_ordinary
);

SELECT 'radix path', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact((a, b)) FROM t_radix_packed_tuple SETTINGS optimize_uniq_exact_radix_partitioning = 1)
WHERE explain LIKE '%RadixUniqExactRouteTransform%';

DROP TABLE t_radix_packed_tuple;
