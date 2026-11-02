-- GROUP BY and DISTINCT over nullable fixed-width keys, for whole blocks and in order for runs of the
-- sorting prefix shorter and longer than the batch-packing threshold, with NULLs only in some blocks and runs.

-- Blocks of 1000 rows: blocks 0, 4, ... have no NULL, blocks 2, 6, ... are all NULL, the others are mixed.
SELECT 'keys128', count(), sum(cityHash64(toString((k, n, s, c)))) FROM
(
    SELECT toUInt64(number % 101) AS k,
        if(intDiv(number, 1000) % 4 = 2 OR (intDiv(number, 1000) % 4 = 1 AND number % 3 = 0), NULL, toUInt32(number % 7)) AS n,
        sum(number) AS s, count() AS c
    FROM numbers(20000) GROUP BY k, n
)
SETTINGS max_block_size = 1000, optimize_aggregation_in_order = 0;

SELECT 'keys256', count(), sum(cityHash64(toString((k, n, s, c)))) FROM
(
    SELECT toUInt64(number % 101) AS k,
        if(intDiv(number, 1000) % 4 = 2 OR (intDiv(number, 1000) % 4 = 3 AND number % 5 = 0), NULL, toUInt64(number % 7)) AS n,
        sum(number) AS s, count() AS c
    FROM numbers(20000) GROUP BY k, n
)
SETTINGS max_block_size = 1000, optimize_aggregation_in_order = 0;

-- A 16-byte nullable key, and a NULL in the first key, which moves the keys after it.
SELECT 'keys256 UInt128 first', count(), sum(cityHash64(toString((w, k, s, c)))) FROM
(
    SELECT if(intDiv(number, 1000) % 4 = 2 OR (intDiv(number, 1000) % 4 = 1 AND number % 3 = 0), NULL, toUInt128(number % 7) * toUInt128('1000000000000000000000')) AS w,
        toUInt64(number % 101) AS k,
        sum(number) AS s, count() AS c
    FROM numbers(20000) GROUP BY w, k
)
SETTINGS max_block_size = 1000, optimize_aggregation_in_order = 0;

SELECT 'keys128 mixed widths', count(), sum(cityHash64(toString((a, b, d, s, c)))) FROM
(
    SELECT if(intDiv(number, 1000) % 4 = 2 OR (intDiv(number, 1000) % 4 = 1 AND number % 3 = 0), NULL, toUInt16(number % 7)) AS a,
        toUInt32(number % 101) AS b,
        if(intDiv(number, 1000) % 4 = 2 OR number % 4 = 0, NULL, toUInt8(number % 5)) AS d,
        sum(number) AS s, count() AS c
    FROM numbers(20000) GROUP BY a, b, d
)
SETTINGS max_block_size = 1000, optimize_aggregation_in_order = 0;

-- Nine nullable keys: the NULL bitmap takes two bytes.
SELECT 'keys128 nine keys', count(), sum(cityHash64(toString((k1, k2, k3, k4, k5, k6, k7, k8, k9, s, c)))) FROM
(
    SELECT if(intDiv(number, 1000) % 4 = 2 OR number % 2 = 0, NULL, toUInt8(number % 3)) AS k1,
        if(intDiv(number, 1000) % 4 = 2, NULL, toUInt8(number % 2)) AS k2,
        if(intDiv(number, 1000) % 4 = 2, NULL, toUInt8(number % 5)) AS k3,
        toUInt8(number % 2) AS k4,
        if(intDiv(number, 1000) % 4 = 2, NULL, toUInt8(number % 3)) AS k5,
        if(intDiv(number, 1000) % 4 = 2, NULL, toUInt8(number % 2)) AS k6,
        if(intDiv(number, 1000) % 4 = 2, NULL, toUInt8(number % 7)) AS k7,
        if(intDiv(number, 1000) % 4 = 2, NULL, toUInt8(number % 2)) AS k8,
        if(intDiv(number, 1000) % 4 = 2 OR (intDiv(number, 1000) % 4 = 3 AND number % 3 = 0), NULL, toUInt8(number % 11)) AS k9,
        sum(number) AS s, count() AS c
    FROM numbers(20000) GROUP BY k1, k2, k3, k4, k5, k6, k7, k8, k9
)
SETTINGS max_block_size = 1000, optimize_aggregation_in_order = 0;

-- One block of runs of 8 rows, then runs of 300; NULLs on run boundaries, run 3 of each 10 long runs
-- is all NULL and run 4 has none.
DROP TABLE IF EXISTS t_nullable_fixed_keys_in_order;
CREATE TABLE t_nullable_fixed_keys_in_order (a UInt64, nb Nullable(UInt64), nc Nullable(UInt32), v UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
             min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_fixed_keys_in_order
SELECT
    if(number < 24000, intDiv(number, 8), 3000 + intDiv(number - 24000, 300)) AS a,
    multiIf(
        number < 24000, if(number % 8 = 0 OR number % 7 = 0, NULL, number % 5),
        intDiv(number - 24000, 300) % 10 = 3, NULL,
        intDiv(number - 24000, 300) % 10 = 4, number % 5,
        if((number - 24000) % 300 IN (0, 299) OR number % 7 = 0, NULL, number % 5)) AS nb,
    multiIf(
        number < 24000, if(number % 8 = 7 OR number % 11 = 0, NULL, number % 3),
        intDiv(number - 24000, 300) % 10 = 3, NULL,
        intDiv(number - 24000, 300) % 10 = 4, number % 3,
        if((number - 24000) % 300 = 299 OR number % 11 = 0, NULL, number % 3)) AS nc,
    number AS v
FROM numbers(48000);
OPTIMIZE TABLE t_nullable_fixed_keys_in_order FINAL;

SELECT 'in order a, nb', count(), sum(cityHash64(toString((a, nb, s, c)))) FROM
(
    SELECT a, nb, sum(v) AS s, count() AS c FROM t_nullable_fixed_keys_in_order GROUP BY a, nb
)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, nb, sum(v) AS s, count() AS c FROM t_nullable_fixed_keys_in_order GROUP BY a, nb
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT 'in order a, nc', count(), sum(cityHash64(toString((a, nc, s, c)))) FROM
(
    SELECT a, nc, sum(v) AS s, count() AS c FROM t_nullable_fixed_keys_in_order GROUP BY a, nc
)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, nc, sum(v) AS s, count() AS c FROM t_nullable_fixed_keys_in_order GROUP BY a, nc
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT 'distinct in order a, nb', count(), sum(cityHash64(toString((a, nb)))) FROM
(
    SELECT DISTINCT a, nb FROM t_nullable_fixed_keys_in_order
)
SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, nb FROM t_nullable_fixed_keys_in_order
    SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%DistinctSortedStreamTransform%';

SELECT 'distinct a, nb', count(), sum(cityHash64(toString((a, nb)))) FROM
(
    SELECT DISTINCT a, nb FROM t_nullable_fixed_keys_in_order
)
SETTINGS optimize_distinct_in_order = 0, max_block_size = 1000;

DROP TABLE t_nullable_fixed_keys_in_order;
