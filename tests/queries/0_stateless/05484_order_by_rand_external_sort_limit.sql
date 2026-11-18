SET query_plan_use_shuffle_for_order_by_rand = 1;
SET max_threads = 1, max_block_size = 128;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;
SET explain_query_plan_default = 'legacy';

-- A finite `LIMIT` must not bypass either external sorting threshold.
SET max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;
SELECT countIf(explain LIKE '%Shuffle%') = 0, countIf(explain LIKE '%Sorting%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5000) ORDER BY rand() LIMIT 1000);
SELECT countIf(explain LIKE '%Shuffle%') = 0, countIf(explain LIKE '%Sorting%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5000) ORDER BY rand64() DESC LIMIT 1000);

-- Exercise the retained sorter with a low spill threshold and check the visible random key order.
SELECT count(), uniqExact(number), arraySort(groupArray(r)) = groupArray(r)
FROM (SELECT number, rand() AS r FROM numbers(5000) ORDER BY r LIMIT 1000);
SELECT count(), uniqExact(number), arrayReverseSort(groupArray(r)) = groupArray(r)
FROM (SELECT number, rand64() AS r FROM numbers(5000) ORDER BY r DESC LIMIT 1000);

-- Select 80% of one million rows under a 100 MB query memory limit.
-- The number and random key occupy about 16 MB before overhead; a 1 MB spill threshold
-- exercises external sorting while leaving headroom for sorting and merge buffers.
SELECT number
FROM numbers(1000000)
ORDER BY rand64()
LIMIT 800000
SETTINGS max_memory_usage = 100000000, max_block_size = 65536,
    max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

SET max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0.5;
SELECT countIf(explain LIKE '%Shuffle%') = 0, countIf(explain LIKE '%Sorting%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5000) ORDER BY rand() LIMIT 1000);
SELECT countIf(explain LIKE '%Shuffle%') = 0, countIf(explain LIKE '%Sorting%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5000) ORDER BY rand64() DESC LIMIT 1000);
