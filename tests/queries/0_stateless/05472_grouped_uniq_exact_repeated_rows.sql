-- Grouped uniqExact skips a row that repeats the place and value of the previous row. Results must not change.
-- Random settings limits: index_granularity=(8192, None); index_granularity_bytes=(10485760, None)

DROP TABLE IF EXISTS t_grouped_uniq_exact;
CREATE TABLE t_grouped_uniq_exact (g UInt32, x UInt64, s String, f Float64, n Nullable(UInt64), c UInt8, ip IPv6)
ENGINE = MergeTree ORDER BY tuple();

-- Runs of repeated (g, x) pairs, interleaved groups, floats with NaN and signed zeros, NULLs.
INSERT INTO t_grouped_uniq_exact SELECT
    intDiv(number, 7) % 13,
    if(number < 60000, intDiv(number, 3), number),
    toString(intDiv(number, 5) % 50000),
    multiIf(number % 11 = 0, nan, number % 11 = 1, -0., number % 11 = 2, 0., toFloat64(intDiv(number, 2) % 1000) / 7),
    if(number % 5 = 0, NULL, intDiv(number, 2) % 70000),
    number % 4 = 0,
    toIPv6(IPv4NumToString(toUInt32(intDiv(number, 3) % 30000)))
FROM numbers(150000);

SELECT g, uniqExact(x), uniqExact(s), uniqExact(f), uniqExact(n), uniqExactIf(x, c), uniqExact(ip), count(DISTINCT x)
FROM t_grouped_uniq_exact GROUP BY g ORDER BY g;

-- Sets that convert to two-level while repeats are skipped: one stream, 110000 keys in group 0 (the threshold is 100000),
-- max_threads > 1 (otherwise sets never convert) and a key wider than 8 bits (an 8-bit key takes another path).
SELECT k, uniqExact(x), uniqExact(toString(x))
FROM (SELECT intDiv(number, 3) AS x, toUInt32(number % 1000 = 999) AS k FROM numbers(330000)) GROUP BY k ORDER BY k
SETTINGS max_threads = 2, optimize_injective_functions_inside_uniq = 0;

-- Small blocks, several threads and two-level aggregation give the same results.
SELECT g, uniqExact(x), uniqExact(s), uniqExact(f) FROM t_grouped_uniq_exact GROUP BY g ORDER BY g
SETTINGS max_block_size = 997, max_threads = 3;
SELECT g, uniqExact(x), uniqExactIf(s, c) FROM t_grouped_uniq_exact GROUP BY g ORDER BY g
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

-- The same counts from distinct pairs.
SELECT (SELECT groupArray((g, u)) FROM (SELECT g, uniqExact(x) AS u FROM t_grouped_uniq_exact GROUP BY g ORDER BY g))
     = (SELECT groupArray((g, u)) FROM (SELECT g, toUInt64(count()) AS u FROM (SELECT DISTINCT g, x FROM t_grouped_uniq_exact) GROUP BY g ORDER BY g));
SELECT (SELECT groupArray((g, u)) FROM (SELECT g, uniqExact(s) AS u FROM t_grouped_uniq_exact GROUP BY g ORDER BY g))
     = (SELECT groupArray((g, u)) FROM (SELECT g, toUInt64(count()) AS u FROM (SELECT DISTINCT g, s FROM t_grouped_uniq_exact) GROUP BY g ORDER BY g));
SELECT (SELECT groupArray((g, u)) FROM (SELECT g, uniqExact(n) AS u FROM t_grouped_uniq_exact GROUP BY g ORDER BY g))
     = (SELECT groupArray((g, u)) FROM (SELECT g, toUInt64(count()) AS u FROM (SELECT DISTINCT g, n FROM t_grouped_uniq_exact WHERE n IS NOT NULL) GROUP BY g ORDER BY g));
SELECT (SELECT groupArray((g, u)) FROM (SELECT g, uniqExactIf(x, c) AS u FROM t_grouped_uniq_exact GROUP BY g ORDER BY g))
     = (SELECT groupArray((g, u)) FROM (SELECT g, toUInt64(count()) AS u FROM (SELECT DISTINCT g, x FROM t_grouped_uniq_exact WHERE c) GROUP BY g ORDER BY g));
SELECT (SELECT groupArray((g, u)) FROM (SELECT g, uniqExact(ip) AS u FROM t_grouped_uniq_exact GROUP BY g ORDER BY g))
     = (SELECT groupArray((g, u)) FROM (SELECT g, toUInt64(count()) AS u FROM (SELECT DISTINCT g, ip FROM t_grouped_uniq_exact) GROUP BY g ORDER BY g));
-- The keyless path agrees with the grouped path.
SELECT (SELECT uniqExact(f) FROM t_grouped_uniq_exact WHERE g = 5) = (SELECT u FROM (SELECT g, uniqExact(f) AS u FROM t_grouped_uniq_exact GROUP BY g) WHERE g = 5);

-- A Nullable condition goes through the -If adapter that folds NULLs into the condition.
SELECT g, uniqExactIf(x, nullIf(c, 0)), uniqExactIf(s, nullIf(c, 0)) FROM t_grouped_uniq_exact GROUP BY g ORDER BY g;

-- Keys that arrive after the GROUP BY limit get no place: k = 2, 3 are dropped.
SELECT intDiv(number, 2500) AS k, uniqExact(intDiv(number, 3) % 1000), uniqExact(toString(intDiv(number, 3) % 1000)),
    uniqExactIf(number % 997, number % 2 = 0), uniqExact(number)
FROM numbers(10000) GROUP BY k ORDER BY k
SETTINGS max_threads = 1, max_block_size = 1024, max_rows_to_group_by = 1, group_by_overflow_mode = 'any',
    optimize_injective_functions_inside_uniq = 0;

DROP TABLE t_grouped_uniq_exact;
