-- `uniqExact` with radix partitioning computes the keys of these types as the values themselves (or their bijection for
-- 128 bits), or as the 128-bit `SipHash` of the others, and must count as many distinct values as without it. Up to 300000
-- distinct values are routed, not kept in the local set of a stream.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SET max_threads = 4, count_distinct_optimization = 0, collect_hash_table_stats_during_aggregation = 0;

SELECT 'Decimal32',
    (SELECT uniqExact(x) FROM (SELECT toDecimal32(number % 300000, 2) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toDecimal32(number % 300000, 2) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toDecimal32(number % 300000, 2) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Decimal64',
    (SELECT uniqExact(x) FROM (SELECT toDecimal64(number % 300000, 4) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toDecimal64(number % 300000, 4) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toDecimal64(number % 300000, 4) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Decimal128',
    (SELECT uniqExact(x) FROM (SELECT toDecimal128(number % 300000, 10) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toDecimal128(number % 300000, 10) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toDecimal128(number % 300000, 10) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Decimal256',
    (SELECT uniqExact(x) FROM (SELECT toDecimal256(number % 300000, 20) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toDecimal256(number % 300000, 20) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toDecimal256(number % 300000, 20) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'DateTime64',
    (SELECT uniqExact(x) FROM (SELECT toDateTime64(number % 300000, 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toDateTime64(number % 300000, 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toDateTime64(number % 300000, 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Time',
    (SELECT uniqExact(x) FROM (SELECT CAST(number % 300000 AS Time) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT CAST(number % 300000 AS Time) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT CAST(number % 300000 AS Time) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Time64',
    (SELECT uniqExact(x) FROM (SELECT toTime64(number % 300000, 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toTime64(number % 300000, 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toTime64(number % 300000, 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Int256',
    (SELECT uniqExact(x) FROM (SELECT toInt256(number % 300000) - 150000 AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toInt256(number % 300000) - 150000 AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toInt256(number % 300000) - 150000 AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'UInt256',
    (SELECT uniqExact(x) FROM (SELECT toUInt256(number % 300000) * toUInt256('1000000000000000000000') AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toUInt256(number % 300000) * toUInt256('1000000000000000000000') AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toUInt256(number % 300000) * toUInt256('1000000000000000000000') AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Nullable(Decimal64)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toDecimal64(number % 300000, 2)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toDecimal64(number % 300000, 2)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toDecimal64(number % 300000, 2)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Nullable(Int256)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toInt256(number % 300000)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toInt256(number % 300000)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toInt256(number % 300000)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'FixedString(4)',
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000 % 10000), 4, '0'), 4) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000 % 10000), 4, '0'), 4) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000 % 10000), 4, '0'), 4) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'FixedString(6)',
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 6, '0'), 6) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 6, '0'), 6) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 6, '0'), 6) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Tuple(UInt64, UInt8)',
    (SELECT uniqExact(x) FROM (SELECT (number % 300000, toUInt8(number % 300000 % 7)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT (number % 300000, toUInt8(number % 300000 % 7)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT (number % 300000, toUInt8(number % 300000 % 7)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Array(UInt64)',
    (SELECT uniqExact(x) FROM (SELECT [number % 300000, number % 300000 % 5] AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT [number % 300000, number % 300000 % 5] AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT [number % 300000, number % 300000 % 5] AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Map(UInt64, UInt64)',
    (SELECT uniqExact(x) FROM (SELECT map(number % 300000, number % 300000 % 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT map(number % 300000, number % 300000 % 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT map(number % 300000, number % 300000 % 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'FixedString(3)',
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000 % 1000), 3, '0'), 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000 % 1000), 3, '0'), 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000 % 1000), 3, '0'), 3) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'FixedString(12)',
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 12, '0'), 12) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 12, '0'), 12) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 12, '0'), 12) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'FixedString(24)',
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 24, '0'), 24) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 24, '0'), 24) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT toFixedString(leftPad(toString(number % 300000), 24, '0'), 24) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'Nullable(FixedString(12))',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(leftPad(toString(number % 300000), 12, '0'), 12)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(leftPad(toString(number % 300000), 12, '0'), 12)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(leftPad(toString(number % 300000), 12, '0'), 12)) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');
