-- GROUP BY, DISTINCT, IN and JOIN over the fixed-width key sets that are batch-packed in clause order: sets with a key
-- of another width than 1, 2, 4, 8 or 16 bytes, nullable sets and keys256 sets. A block of 100000 rows holds several
-- chunks of a set packed in chunks. In order, runs of the sorting prefix are shorter and longer than 256 rows.

DROP TABLE IF EXISTS t_keys_fixed_chunks;
DROP TABLE IF EXISTS t_keys_fixed_chunks_in_order;
DROP TABLE IF EXISTS t_keys_fixed_chunks_fan_out;

CREATE TABLE t_keys_fixed_chunks
(
    a Int64, b Int64, c Int64, cn Nullable(Int64), c32 UInt32,
    f3 FixedString(3), f3n Nullable(FixedString(3)), f20 FixedString(20), f20n Nullable(FixedString(20)), w UInt16, u UInt8, q UInt32
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_keys_fixed_chunks
SELECT
    n, n * 7, n * 13, if(number % 9 = 3, NULL, n * 13), toUInt32(n),
    toFixedString(reinterpretAsString(toUInt32(n)), 3) AS f, if(number % 17 = 5, NULL, f),
    toFixedString(concat(f, 'abcdefghijklmnopq'), 20) AS f20, if(number % 13 = 4, NULL, f20),
    bitAnd(n, 65535), number % 5, number % 3
FROM (SELECT number, toInt64(number % 100100) AS n FROM numbers(300000));

-- Runs of `a` cycle over the lengths 1, 7, 255, 256, 300 and 600.
CREATE TABLE t_keys_fixed_chunks_in_order (a Int64, b Int64, cn Nullable(Int64), c32 UInt32, f3 FixedString(3), u UInt8)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_keys_fixed_chunks_in_order
SELECT
    intDiv(number, 1419) * 6 + multiIf(number % 1419 < 1, 0, number % 1419 < 8, 1, number % 1419 < 263, 2, number % 1419 < 519, 3, number % 1419 < 819, 4, 5),
    (number % 11) * 1000000000007, if(number % 13 = 2, NULL, number % 17), toUInt32(number % 37) * 1000003,
    toFixedString(reinterpretAsString(toUInt32(number % 70001)), 3), number % 3
FROM numbers(1419 * 35);
OPTIMIZE TABLE t_keys_fixed_chunks_in_order FINAL;

-- 100 rows per key, so that a probe block is cut by `max_joined_block_size_rows` and resumed many times.
CREATE TABLE t_keys_fixed_chunks_fan_out (f3 FixedString(3), u UInt8, k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_keys_fixed_chunks_fan_out
SELECT toFixedString(reinterpretAsString(toUInt32(intDiv(number, 100) * 7)), 3), intDiv(number, 100) % 5, number FROM numbers(100000);

SET max_block_size = 100000, preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;

-- Keys of other widths, with and without Nullable.
SELECT 'group by f3, u, q', count(), sum(cityHash64(toString((f3, u, q, n)))) FROM (SELECT f3, u, q, count() AS n FROM t_keys_fixed_chunks GROUP BY f3, u, q);
SELECT 'group by f3, u', count(), sum(cityHash64(toString((f3, u, n)))) FROM (SELECT f3, u, count() AS n FROM t_keys_fixed_chunks GROUP BY f3, u);
SELECT 'group by f3n, u, q', count(), sum(cityHash64(toString((f3n, u, q, n)))) FROM (SELECT f3n, u, q, count() AS n FROM t_keys_fixed_chunks GROUP BY f3n, u, q);
SELECT 'group by f20, w', count(), sum(cityHash64(toString((f20, w, n)))) FROM (SELECT f20, w, count() AS n FROM t_keys_fixed_chunks GROUP BY f20, w);
SELECT 'group by f20n, w', count(), sum(cityHash64(toString((f20n, w, n)))) FROM (SELECT f20n, w, count() AS n FROM t_keys_fixed_chunks GROUP BY f20n, w);
SELECT 'two-level external f3, u, q', count(), sum(cityHash64(toString((f3, u, q, n)))) FROM (SELECT f3, u, q, count() AS n FROM t_keys_fixed_chunks GROUP BY f3, u, q)
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
SELECT 'top-k f3, u, q', groupArray((hex(f3), u, q, n)) FROM
(SELECT f3, u, q, count() AS n FROM t_keys_fixed_chunks GROUP BY f3, u, q ORDER BY f3, u, q LIMIT 10)
SETTINGS enable_group_by_top_k_optimization = 1;
SELECT 'distinct f3, u, q', count(), sum(cityHash64(toString((f3, u, q)))) FROM (SELECT DISTINCT f3, u, q FROM t_keys_fixed_chunks);
SELECT 'distinct f3n, u, q', count(), sum(cityHash64(toString((f3n, u, q)))) FROM (SELECT DISTINCT f3n, u, q FROM t_keys_fixed_chunks);
SELECT 'distinct f20, w', count(), sum(cityHash64(toString((f20, w)))) FROM (SELECT DISTINCT f20, w FROM t_keys_fixed_chunks);
SELECT 'distinct f20n, w', count(), sum(cityHash64(toString((f20n, w)))) FROM (SELECT DISTINCT f20n, w FROM t_keys_fixed_chunks);
SELECT 'in f3, u', count(), sum(cityHash64(toString((a, q)))) FROM t_keys_fixed_chunks
WHERE (f3, u) IN (SELECT f3, u FROM t_keys_fixed_chunks WHERE a % 3 = 0);
SELECT 'in f3n, u', count(), sum(cityHash64(toString((a, q)))) FROM t_keys_fixed_chunks
WHERE (f3n, u) IN (SELECT f3n, u FROM t_keys_fixed_chunks WHERE a % 3 = 0);
SELECT 'in f3n, u transform_null_in', count(), sum(cityHash64(toString((a, q)))) FROM t_keys_fixed_chunks
WHERE (f3n, u) IN (SELECT f3n, u FROM t_keys_fixed_chunks WHERE a % 3 = 0) SETTINGS transform_null_in = 1;
SELECT 'join hash f3, u', count(), sum(cityHash64(toString((l.a, l.q, r.k)))) FROM t_keys_fixed_chunks AS l
INNER JOIN (SELECT DISTINCT f3, u, a % 5 AS k FROM t_keys_fixed_chunks WHERE q = 1) AS r ON l.f3 = r.f3 AND l.u = r.u
SETTINGS join_algorithm = 'hash';
SELECT 'join parallel_hash f3, u', count(), sum(cityHash64(toString((l.a, l.q, r.k)))) FROM t_keys_fixed_chunks AS l
INNER JOIN (SELECT DISTINCT f3, u, a % 5 AS k FROM t_keys_fixed_chunks WHERE q = 1) AS r ON l.f3 = r.f3 AND l.u = r.u
SETTINGS join_algorithm = 'parallel_hash', max_threads = 8;
SELECT 'join fan-out hash f3, u', count(), sum(cityHash64(toString((l.a, l.q, r.k)))) FROM t_keys_fixed_chunks AS l
INNER JOIN t_keys_fixed_chunks_fan_out AS r ON l.f3 = r.f3 AND l.u = r.u
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 1000;
SELECT 'join fan-out parallel_hash f3, u', count(), sum(cityHash64(toString((l.a, l.q, r.k)))) FROM t_keys_fixed_chunks AS l
INNER JOIN t_keys_fixed_chunks_fan_out AS r ON l.f3 = r.f3 AND l.u = r.u
SETTINGS join_algorithm = 'parallel_hash', max_threads = 8, max_joined_block_size_rows = 1000;
SELECT 'join hash build with NULL keys f3n, u', count(), sum(cityHash64(toString((l.a, l.q, r.k)))) FROM t_keys_fixed_chunks AS l
INNER JOIN (SELECT f3n, u, a % 5 AS k FROM t_keys_fixed_chunks WHERE q = 1) AS r ON l.f3 = r.f3n AND l.u = r.u
SETTINGS join_algorithm = 'hash';
SELECT 'asof join build with NULL asof keys f3, u', count(), sum(cityHash64(toString((l.a, l.q, r.t)))) FROM t_keys_fixed_chunks AS l
ASOF INNER JOIN
(
    SELECT toFixedString(reinterpretAsString(toUInt32(number % 100100)), 3) AS f3, toUInt8(number % 5) AS u,
        if(number % 100000 >= 60000 OR number >= 200000, NULL, toInt64(number % 100100)) AS t
    FROM numbers(300000)
) AS r ON l.f3 = r.f3 AND l.u = r.u AND l.a >= r.t
SETTINGS join_algorithm = 'hash', hash_join_layout = 'serial', query_plan_join_swap_table = false;
SELECT 'asof join parallel build with NULL asof keys f3, u', count(), sum(cityHash64(toString((l.a, l.q, r.t)))) FROM t_keys_fixed_chunks AS l
ASOF INNER JOIN
(
    SELECT toFixedString(reinterpretAsString(toUInt32(number % 100100)), 3) AS f3, toUInt8(number % 5) AS u,
        if(number % 100000 >= 60000 OR number >= 200000, NULL, toInt64(number % 100100)) AS t
    FROM numbers(300000)
) AS r ON l.f3 = r.f3 AND l.u = r.u AND l.a >= r.t
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_threads = 8, query_plan_join_swap_table = false;
SELECT 'in order a, f3, u', count(), sum(cityHash64(toString((a, f3, u, n)))) FROM
(SELECT a, f3, u, count() AS n FROM t_keys_fixed_chunks_in_order GROUP BY a, f3, u)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1;
SELECT 'distinct in order a, f3, u', count(), sum(cityHash64(toString((a, f3, u)))) FROM
(SELECT DISTINCT a, f3, u FROM t_keys_fixed_chunks_in_order)
SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, f3, u, count() FROM t_keys_fixed_chunks_in_order GROUP BY a, f3, u
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1)
WHERE explain ILIKE '%AggregatingInOrderTransform%';
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, f3, u FROM t_keys_fixed_chunks_in_order
    SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1)
WHERE explain ILIKE '%DistinctSortedStreamTransform%';

DROP TABLE t_keys_fixed_chunks_fan_out;
DROP TABLE t_keys_fixed_chunks_in_order;
DROP TABLE t_keys_fixed_chunks;
