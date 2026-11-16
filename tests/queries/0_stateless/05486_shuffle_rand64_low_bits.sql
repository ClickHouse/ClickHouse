SET query_plan_use_shuffle_for_order_by_rand = 1;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;
SET max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;
SET max_threads = 1, max_block_size = 1000;

-- Limited samples must retain the sorter until generated keys preserve every bit of `rand64`.
SELECT count() = 0 FROM (EXPLAIN SELECT rand64() AS r FROM numbers(100000) ORDER BY r LIMIT 100000) WHERE explain LIKE '%Shuffle%';
SELECT count() = 0 FROM (EXPLAIN SELECT rand64() AS r FROM numbers(100000) ORDER BY r DESC LIMIT 100000) WHERE explain LIKE '%Shuffle%';

-- Around 98 of 100000 uniform keys have ten zero low bits. Floating-point keys fail these loose bounds.
SELECT count() = 100000, countIf(bitAnd(r, 1023) = 0) BETWEEN 10 AND 300,
    countIf(bitAnd(r, 1) = 1) BETWEEN 45000 AND 55000
FROM (SELECT rand64() AS r FROM numbers(100000) ORDER BY r LIMIT 100000);

SELECT count() = 100000, countIf(bitAnd(r, 1023) = 0) BETWEEN 10 AND 300,
    countIf(bitAnd(r, 1) = 1) BETWEEN 45000 AND 55000
FROM (SELECT rand64() AS r FROM numbers(100000) ORDER BY r DESC LIMIT 100000);

-- The unlimited shuffle uses integer keys and must preserve their low bits as well.
SELECT count() = 100000, countIf(bitAnd(r, 1023) = 0) BETWEEN 10 AND 300,
    countIf(bitAnd(r, 1) = 1) BETWEEN 45000 AND 55000
FROM (SELECT rand64() AS r FROM numbers(100000) ORDER BY r);
