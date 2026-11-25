-- `uniqExact` with radix partitioning hashes the strings of 16 bytes or more of a block, and the `FixedString` values
-- longer than 16 bytes other than `FixedString(32)`, several at a time. It must count as many distinct values as without it.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SET max_threads = 4, count_distinct_optimization = 0, collect_hash_table_stats_during_aggregation = 0, optimize_injective_functions_inside_uniq = 0;

SELECT 'long strings, repeated',
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 1000), ':', repeat('x', 16 + number % 1000 % 285)) AS x FROM numbers_mt(100000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 1000), ':', repeat('x', 16 + number % 1000 % 285)) AS x FROM numbers_mt(100000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0),
    (SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM (SELECT concat(toString(number), repeat('x', 20)) AS x FROM numbers_mt(1000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
        WHERE explain LIKE '%RadixUniqExactRouteTransform%');

SELECT 'short and long strings interleaved',
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 30000), repeat('y', number % 32)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 30000), repeat('y', number % 32)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'short strings, then long strings',
    (SELECT uniqExact(x) FROM (SELECT if(number % 256 < 128, toString(number % 50000), concat(toString(number % 50000), repeat('z', 20))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 256 < 128, toString(number % 50000), concat(toString(number % 50000), repeat('z', 20))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'consecutive repeats',
    (SELECT uniqExact(x) FROM (SELECT concat(toString(intDiv(number, 3)), repeat('r', 17 + intDiv(number, 3) % 3)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT concat(toString(intDiv(number, 3)), repeat('r', 17 + intDiv(number, 3) % 3)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'three alternating long strings',
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 3), repeat('a', 40)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 3), repeat('a', 40)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'empty strings between long ones',
    (SELECT uniqExact(x) FROM (SELECT if(number % 4 = 0, '', concat(toString(number % 40000), repeat('e', 30))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 4 = 0, '', concat(toString(number % 40000), repeat('e', 30))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'Nullable(String)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 5 = 0, NULL, concat(toString(number % 40000), repeat('n', 1 + number % 40))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 5 = 0, NULL, concat(toString(number % 40000), repeat('n', 1 + number % 40))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'LowCardinality(String)',
    (SELECT uniqExact(x) FROM (SELECT toLowCardinality(concat(toString(number % 3000), repeat('l', 1 + number % 3000 % 40))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toLowCardinality(concat(toString(number % 3000), repeat('l', 1 + number % 3000 % 40))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'LowCardinality(Nullable(String))',
    (SELECT uniqExact(x) FROM (SELECT toLowCardinality(if(number % 7 = 0, NULL, concat(toString(number % 3000), repeat('l', 20)))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT toLowCardinality(if(number % 7 = 0, NULL, concat(toString(number % 3000), repeat('l', 20)))) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'strings of 512 to 4100 bytes and short ones',
    (SELECT uniqExact(x) FROM (SELECT if(number % 3 = 0, toString(number % 500), concat(toString(number % 500), repeat('g', 512 + number % 500 * 37 % 3589))) AS x FROM numbers_mt(5000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 3 = 0, toString(number % 500), concat(toString(number % 500), repeat('g', 512 + number % 500 * 37 % 3589))) AS x FROM numbers_mt(5000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'FixedString(16)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 16)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 16)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'FixedString(17)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 17)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 17)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'FixedString(32)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 32)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 32)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'FixedString(64)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 64)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 64)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'FixedString(100)',
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 100)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 7 = 0, NULL, toFixedString(toString(intDiv(number, 3) % 40000), 100)) AS x FROM numbers_mt(200000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

-- 16 strings of 23 bytes are hashed in two groups. The string of 15 bytes would equal the key of the one ending in 494
-- if the hash of the longer strings did not have the top bit set.
SELECT 'short string among a group of long ones',
    (SELECT uniqExact(s) FROM (SELECT arrayJoin(arrayConcat([unhex('D7A0004D354CD8BEB38E8080053CB7')], arrayMap(i -> concat('long-string-example-', toString(i)), range(480, 496)))) AS s) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(s) FROM (SELECT arrayJoin(arrayConcat(arrayMap(i -> concat('long-string-example-', toString(i)), range(480, 496)), [unhex('D7A0004D354CD8BEB38E8080053CB7')])) AS s) SETTINGS optimize_uniq_exact_radix_partitioning = 1);

SELECT 'blocks of 7 rows',
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 1000), ':', repeat('x', 16 + number % 1000 % 285)) AS x FROM numbers_mt(20000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 7),
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 3000), repeat('y', number % 32)) AS x FROM numbers_mt(20000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 7),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(toString(intDiv(number, 3) % 3000), 64) AS x FROM numbers_mt(20000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 7);

SELECT 'blocks of 257 rows',
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 1000), ':', repeat('x', 16 + number % 1000 % 285)) AS x FROM numbers_mt(20000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 257),
    (SELECT uniqExact(x) FROM (SELECT concat(toString(number % 3000), repeat('y', number % 32)) AS x FROM numbers_mt(20000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 257),
    (SELECT uniqExact(x) FROM (SELECT toFixedString(toString(intDiv(number, 3) % 3000), 64) AS x FROM numbers_mt(20000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 257);
