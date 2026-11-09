-- GROUP BY, DISTINCT, IN and JOIN over non-nullable fixed-width keys of 17 to 32 bytes with a key narrower
-- than 8 bytes, for whole blocks, for two-level and external aggregation, and in order for runs of the
-- sorting prefix shorter and longer than the batch-packing threshold.

DROP TABLE IF EXISTS t_fixed_keys256;
DROP TABLE IF EXISTS t_fixed_keys256_join;

-- Runs of `a` cycle over the lengths 1, 7, 255, 256, 300 and 600.
CREATE TABLE t_fixed_keys256
(
    a UInt64, c32 UInt32, b UInt64, c UInt64, w UInt128,
    k1 UInt16, k2 UInt16, k3 UInt16, k4 UInt16, k5 UInt16, k6 UInt16, k7 UInt16, k8 UInt16, k9 UInt16
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_fixed_keys256
SELECT
    intDiv(number, 1419) * 6 + multiIf(number % 1419 < 1, 0, number % 1419 < 8, 1, number % 1419 < 263, 2,
                                       number % 1419 < 519, 3, number % 1419 < 819, 4, 5) AS a,
    toUInt32(number % 37) * 1000003 AS c32,
    (number % 11) * 1000000000007 AS b,
    number % 3 AS c,
    toUInt128(number % 5) * toUInt128('1000000000000000000000') + number % 3 AS w,
    number % 3, number % 2, number % 5, number % 2, number % 3, number % 7, number % 2, number % 3, number % 4
FROM numbers(1419 * 35);
OPTIMIZE TABLE t_fixed_keys256 FINAL;

SELECT 'in order a, c32, b', count(), sum(cityHash64(toString((a, c32, b, s, n)))) FROM
(
    SELECT a, c32, b, sum(c) AS s, count() AS n FROM t_fixed_keys256 GROUP BY a, c32, b
)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, c32, b, sum(c) AS s, count() AS n FROM t_fixed_keys256 GROUP BY a, c32, b
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT 'a, c32, b', count(), sum(cityHash64(toString((a, c32, b, s, n)))) FROM
(
    SELECT a, c32, b, sum(c) AS s, count() AS n FROM t_fixed_keys256 GROUP BY a, c32, b
)
SETTINGS optimize_aggregation_in_order = 0;

SELECT 'two-level external a, c32, b', count(), sum(cityHash64(toString((a, c32, b, s, n)))) FROM
(
    SELECT a, c32, b, sum(c) AS s, count() AS n FROM t_fixed_keys256 GROUP BY a, c32, b
)
SETTINGS optimize_aggregation_in_order = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
         max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'distinct in order a, c32, b, c', count(), sum(cityHash64(toString((a, c32, b, c)))) FROM
(
    SELECT DISTINCT a, c32, b, c FROM t_fixed_keys256
)
SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, c32, b, c FROM t_fixed_keys256
    SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 100000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%DistinctSortedStreamTransform%';

SELECT 'distinct c32, b, c', count(), sum(cityHash64(toString((c32, b, c)))) FROM
(
    SELECT DISTINCT c32, b, c FROM t_fixed_keys256
)
SETTINGS optimize_distinct_in_order = 0;

SELECT 'in', count(), sum(cityHash64(toString((a, c32, b, c)))) FROM t_fixed_keys256
WHERE (c32, b, c) IN (SELECT c32, b, c FROM t_fixed_keys256 WHERE a % 3 = 0 AND c32 % 2 = 0);

SELECT 'join hash', count(), sum(cityHash64(toString((l.a, c32, b, c, r.a)))) FROM t_fixed_keys256 AS l
INNER JOIN (SELECT DISTINCT c32, b, c, a % 5 AS a FROM t_fixed_keys256) AS r USING (c32, b, c)
SETTINGS join_algorithm = 'hash';

SELECT 'join parallel_hash', count(), sum(cityHash64(toString((l.a, c32, b, c, r.a)))) FROM t_fixed_keys256 AS l
INNER JOIN (SELECT DISTINCT c32, b, c, a % 5 AS a FROM t_fixed_keys256) AS r USING (c32, b, c)
SETTINGS join_algorithm = 'parallel_hash';

-- The Join engine keeps the packed keys and unpacks them when the table is read.
CREATE TABLE t_fixed_keys256_join (c32 UInt32, b UInt64, c UInt64, v UInt64) ENGINE = Join(ANY, LEFT, c32, b, c);
INSERT INTO t_fixed_keys256_join SELECT c32, b, c, c32 + b * 3 + c * 7 FROM t_fixed_keys256;

SELECT 'join table', count(), sum(cityHash64(toString((c32, b, c, v)))) FROM t_fixed_keys256_join;
SELECT 'joinGet', count(), sum(cityHash64(toString((a, joinGet('t_fixed_keys256_join', 'v', c32, b, c))))) FROM t_fixed_keys256;

-- A 16-byte key with a 4-byte one, and nine 2-byte keys.
SELECT 'w, c32', count(), sum(cityHash64(toString((w, c32, n)))) FROM
(
    SELECT w, c32, count() AS n FROM t_fixed_keys256 GROUP BY w, c32
);

SELECT 'nine UInt16', count(), sum(cityHash64(toString((k1, k2, k3, k4, k5, k6, k7, k8, k9, n)))) FROM
(
    SELECT k1, k2, k3, k4, k5, k6, k7, k8, k9, count() AS n FROM t_fixed_keys256 GROUP BY k1, k2, k3, k4, k5, k6, k7, k8, k9
);

DROP TABLE t_fixed_keys256_join;
DROP TABLE t_fixed_keys256;
