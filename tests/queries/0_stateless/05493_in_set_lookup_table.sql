-- IN and NOT IN once the keys of the set are looked up in its lookup table, which is built after 16 probed rows per
-- key. Each check compares the result with an arithmetic oracle, so every line is 1.
SET max_block_size = 1024, max_threads = 2, max_bytes_before_external_set = 0, max_bytes_ratio_before_external_set = 0;

-- UInt64, set {0, 3, ..., 2997}.
SELECT countIf(x IN (SELECT number * 3 FROM numbers(1000))) = countIf(x % 3 = 0 AND x < 3000)
    AND countIf(x NOT IN (SELECT number * 3 FROM numbers(1000))) = countIf(NOT (x % 3 = 0 AND x < 3000))
FROM (SELECT intHash64(number) % 6000 AS x FROM numbers(40000));

-- Int32, set {0, -3, ..., -2997}.
SELECT countIf(x IN (SELECT toInt32(-number * 3) FROM numbers(1000))) = countIf(x <= 0 AND x >= -2997 AND x % 3 = 0)
    AND countIf(x NOT IN (SELECT toInt32(-number * 3) FROM numbers(1000))) = countIf(NOT (x <= 0 AND x >= -2997 AND x % 3 = 0))
FROM (SELECT toInt32(intHash64(number) % 6000) - 3000 AS x FROM numbers(40000));

-- Float64 with NaN, set {nan, 0.5, 1, ..., 499.5}.
SELECT countIf(x IN (SELECT if(number = 0, nan, number * 0.5) FROM numbers(1000))) = countIf(isNaN(x) OR (x >= 0.5 AND x <= 499.5))
    AND countIf(x NOT IN (SELECT if(number = 0, nan, number * 0.5) FROM numbers(1000))) = countIf(NOT (isNaN(x) OR (x >= 0.5 AND x <= 499.5)))
FROM (SELECT if(number % 97 = 0, nan, (intHash64(number) % 2000) * 0.5) AS x FROM numbers(40000));

-- (UInt64, UInt32), packed into 128 bits.
SELECT countIf((a, b) IN (SELECT number * 3, toUInt32(number % 7) FROM numbers(1000))) = countIf(a % 3 = 0 AND a < 3000 AND b = intDiv(a, 3) % 7)
    AND countIf((a, b) NOT IN (SELECT number * 3, toUInt32(number % 7) FROM numbers(1000))) = countIf(NOT (a % 3 = 0 AND a < 3000 AND b = intDiv(a, 3) % 7))
FROM (SELECT intHash64(number) % 6000 AS a, toUInt32(if(number % 2 = 0, intDiv(a, 3) % 7, intHash64(number + 1) % 14)) AS b FROM numbers(40000));

-- (UInt64, UInt64).
SELECT countIf((a, b) IN (SELECT number * 3, number * 5 FROM numbers(1000))) = countIf(a % 3 = 0 AND a < 3000 AND b = intDiv(a, 3) * 5)
    AND countIf((a, b) NOT IN (SELECT number * 3, number * 5 FROM numbers(1000))) = countIf(NOT (a % 3 = 0 AND a < 3000 AND b = intDiv(a, 3) * 5))
FROM (SELECT intHash64(number) % 6000 AS a, if(number % 2 = 0, intDiv(a, 3) * 5, a) AS b FROM numbers(40000));

-- (UInt32, UInt32), packed into 64 bits.
SELECT countIf((a, b) IN (SELECT toUInt32(number * 3), toUInt32(number * 5) FROM numbers(1000))) = countIf(a % 3 = 0 AND a < 3000 AND b = intDiv(a, 3) * 5)
    AND countIf((a, b) NOT IN (SELECT toUInt32(number * 3), toUInt32(number * 5) FROM numbers(1000))) = countIf(NOT (a % 3 = 0 AND a < 3000 AND b = intDiv(a, 3) * 5))
FROM (SELECT toUInt32(intHash64(number) % 6000) AS a, toUInt32(if(number % 2 = 0, intDiv(a, 3) * 5, a)) AS b FROM numbers(40000));

-- FixedString(10).
SELECT countIf(toFixedString(toString(x), 10) IN (SELECT toFixedString(toString(number * 3), 10) FROM numbers(1000))) = countIf(x % 3 = 0 AND x < 3000)
    AND countIf(toFixedString(toString(x), 10) NOT IN (SELECT toFixedString(toString(number * 3), 10) FROM numbers(1000))) = countIf(NOT (x % 3 = 0 AND x < 3000))
FROM (SELECT intHash64(number) % 6000 AS x FROM numbers(40000));

-- UUID.
SELECT countIf(reinterpretAsUUID(toUInt128(x)) IN (SELECT reinterpretAsUUID(toUInt128(number * 3)) FROM numbers(1000))) = countIf(x % 3 = 0 AND x < 3000)
    AND countIf(reinterpretAsUUID(toUInt128(x)) NOT IN (SELECT reinterpretAsUUID(toUInt128(number * 3)) FROM numbers(1000))) = countIf(NOT (x % 3 = 0 AND x < 3000))
FROM (SELECT intHash64(number) % 6000 AS x FROM numbers(40000));

-- Nullable(UInt64) with NULL rows, whose nested values are also in the set, and NULL in the set.
SELECT countIf(x IN (SELECT if(number = 0, NULL, number * 3) FROM numbers(1000))) = countIf(x IS NOT NULL AND x % 3 = 0 AND x > 0 AND x < 3000)
    AND countIf(x NOT IN (SELECT if(number = 0, NULL, number * 3) FROM numbers(1000))) = countIf(x IS NOT NULL AND NOT (x % 3 = 0 AND x > 0 AND x < 3000))
FROM (SELECT if(number % 11 = 0, NULL, intHash64(number) % 6000) AS x FROM numbers(40000))
SETTINGS transform_null_in = 0;

SELECT countIf(x IN (SELECT if(number = 0, NULL, number * 3) FROM numbers(1000))) = countIf(x IS NULL OR (x % 3 = 0 AND x > 0 AND x < 3000))
    AND countIf(x NOT IN (SELECT if(number = 0, NULL, number * 3) FROM numbers(1000))) = countIf(NOT (x IS NULL OR (x % 3 = 0 AND x > 0 AND x < 3000)))
FROM (SELECT if(number % 11 = 0, NULL, intHash64(number) % 6000) AS x FROM numbers(40000))
SETTINGS transform_null_in = 1;

-- A tuple with a Nullable element: a row with a NULL key is not in the set, whatever the nested values are.
SELECT countIf((x, y) IN (SELECT number * 3, toUInt32(number % 7) FROM numbers(1000))) = countIf(x IS NOT NULL AND x % 3 = 0 AND x < 3000 AND y = intDiv(x, 3) % 7)
    AND countIf((x, y) NOT IN (SELECT number * 3, toUInt32(number % 7) FROM numbers(1000))) = countIf(NOT (x IS NOT NULL AND x % 3 = 0 AND x < 3000 AND y = intDiv(x, 3) % 7))
FROM
(
    SELECT intHash64(number) % 6000 AS v, if(number % 11 = 0, NULL, v) AS x,
        toUInt32(if(number % 2 = 0, intDiv(v, 3) % 7, intHash64(number + 1) % 14)) AS y
    FROM numbers(40000)
)
SETTINGS transform_null_in = 0;

-- A dense set of 30000 keys.
SELECT countIf(x IN (SELECT number FROM numbers(30000))) = countIf(x < 30000)
    AND countIf(x NOT IN (SELECT number FROM numbers(30000))) = countIf(x >= 30000)
FROM (SELECT intHash64(number) % 60000 AS x FROM numbers(600000));

-- Random keys, then runs of 100 equal keys.
SELECT countIf(x IN (SELECT number * 3 FROM numbers(1000))) = countIf(x % 3 = 0 AND x < 3000)
    AND countIf(x NOT IN (SELECT number * 3 FROM numbers(1000))) = countIf(NOT (x % 3 = 0 AND x < 3000))
FROM (SELECT if(number < 200000, intHash64(number), intDiv(number, 100)) % 6000 AS x FROM numbers(400000));

-- An exact JOIN runtime filter.
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_runtime_filter_exact_values_limit = 10000,
    join_runtime_filter_from_fixed_hash_table = 0;

SELECT count() = (SELECT countIf(k % 3 = 0 AND k < 3000) FROM (SELECT toUInt32(intHash64(number) % 6000) AS k FROM numbers(40000)))
FROM (SELECT toUInt32(intHash64(number) % 6000) AS k FROM numbers(40000)) AS l
INNER JOIN (SELECT toUInt32(number * 3) AS k FROM numbers(1000)) AS r ON l.k = r.k;

-- Most probe rows have a match, so that the filter of the anti join stays selective and keeps checking rows.
SELECT count() = (SELECT countIf(k >= 3000) FROM (SELECT toUInt32(intHash64(number) % 1100 * 3) AS k FROM numbers(40000)))
FROM (SELECT toUInt32(intHash64(number) % 1100 * 3) AS k FROM numbers(40000)) AS l
LEFT ANTI JOIN (SELECT toUInt32(number * 3) AS k FROM numbers(1000)) AS r ON l.k = r.k;
