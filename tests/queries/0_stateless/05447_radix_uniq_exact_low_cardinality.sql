-- `uniqExact` with radix partitioning routes the distinct dictionary values that the rows of each block use, and
-- must count as many distinct values as without it. NULLs are skipped.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SET max_threads = 4, count_distinct_optimization = 0, collect_hash_table_stats_during_aggregation = 0;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(toLowCardinality(toString(number))) FROM numbers_mt(10) SETTINGS optimize_uniq_exact_radix_partitioning = 1)
WHERE explain LIKE '%RadixUniqExactRouteTransform%';

SELECT 'LowCardinality(String)',
    (SELECT uniqExact(toLowCardinality(toString(number % 30000))) FROM numbers_mt(200000) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(toLowCardinality(toString(number % 30000))) FROM numbers_mt(200000) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'LowCardinality(Nullable(String))',
    (SELECT uniqExact(toLowCardinality(if(number % 7 = 0, NULL, toString(number % 1000)))) FROM numbers_mt(200000) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(toLowCardinality(if(number % 7 = 0, NULL, toString(number % 1000)))) FROM numbers_mt(200000) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'LowCardinality(FixedString(3))',
    (SELECT uniqExact(toLowCardinality(toFixedString(toString(number % 1000), 3))) FROM numbers_mt(200000) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(toLowCardinality(toFixedString(toString(number % 1000), 3))) FROM numbers_mt(200000) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

-- The blocks read from the table share dictionaries larger than themselves.
DROP TABLE IF EXISTS low_cardinality_values;
CREATE TABLE low_cardinality_values (s LowCardinality(String), n LowCardinality(Nullable(String)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO low_cardinality_values SELECT toString(number % 5000), if(number % 3 = 0, NULL, toString(number % 300)) FROM numbers(300000);

SELECT 'MergeTree', uniqExact(s), uniqExact(n) FROM low_cardinality_values SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_block_size = 777;
SELECT 'MergeTree', uniqExact(s), uniqExact(n) FROM low_cardinality_values SETTINGS optimize_uniq_exact_radix_partitioning = 0, max_block_size = 777;

-- Only NULLs: the result is 0, not an empty set.
SELECT 'only NULLs', uniqExact(n) FROM low_cardinality_values WHERE n IS NULL
SETTINGS optimize_uniq_exact_radix_partitioning = 1, empty_result_for_aggregation_by_empty_set = 1;

DROP TABLE low_cardinality_values;
