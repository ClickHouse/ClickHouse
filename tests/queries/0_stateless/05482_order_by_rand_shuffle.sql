-- `ORDER BY rand()` is executed by shuffling (the setting `query_plan_use_shuffle_for_order_by_rand`).
-- The values of the `rand()` column are replaced by other random values, so the result must still be sorted by them.

SET query_plan_use_shuffle_for_order_by_rand = 1;
SET max_threads = 4, max_block_size = 1000;

SELECT 'Plan';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY rand() LIMIT 10) WHERE explain LIKE '%Shuffle%';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY rand64() DESC LIMIT 10) WHERE explain LIKE '%Shuffle%';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY rand() SETTINGS max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0) WHERE explain LIKE '%Shuffle%';
-- Not applied: the sorting may spill to disk.
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY rand() SETTINGS max_bytes_before_external_sort = 1000000) WHERE explain LIKE '%Shuffle%';
-- Not applied: not a random sort key, or several sort keys, or the setting is disabled.
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY number LIMIT 10) WHERE explain LIKE '%Shuffle%';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY rand(), number LIMIT 10) WHERE explain LIKE '%Shuffle%';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY randCanonical() LIMIT 10) WHERE explain LIKE '%Shuffle%';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(1000) ORDER BY rand() LIMIT 10 SETTINGS query_plan_use_shuffle_for_order_by_rand = 0) WHERE explain LIKE '%Shuffle%';

SELECT 'The result is sorted by the random values';
SELECT count(), arraySort(groupArray(r)) = groupArray(r), uniqExact(number) FROM (SELECT number, rand() AS r FROM numbers_mt(100000) ORDER BY r SETTINGS max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0);
SELECT count(), arrayReverseSort(groupArray(r)) = groupArray(r), uniqExact(number) FROM (SELECT number, rand64() AS r FROM numbers_mt(100000) ORDER BY r DESC LIMIT 1000);
SELECT count(), arraySort(groupArray(r)) = groupArray(r) FROM (SELECT rand() AS r FROM numbers_mt(100000) ORDER BY r LIMIT 10);
SELECT count(), arraySort(groupArray(r)) = groupArray(r) FROM (SELECT rand() AS r FROM remote('127.0.0.{1,2}', numbers(1000)) ORDER BY r LIMIT 100);

SELECT 'Other uses of the random value are consistent';
SELECT count(), countIf(m != r % 10) FROM (SELECT rand() AS r, r % 10 AS m FROM numbers_mt(10000) ORDER BY r LIMIT 1000);
SELECT count(), countIf(r % 2 != 0) FROM (SELECT number, r FROM (SELECT number, rand() AS r FROM numbers_mt(10000)) WHERE r % 2 = 0 ORDER BY r LIMIT 1000);

SELECT 'The sample is uniform';
-- The standard deviation of every count is about 95.
SELECT count() = 10, min(c) > 9000, max(c) < 11000
FROM (SELECT intDiv(number, 100000) AS bucket, count() AS c FROM (SELECT number FROM numbers_mt(1000000) ORDER BY rand() LIMIT 100000) GROUP BY bucket);
