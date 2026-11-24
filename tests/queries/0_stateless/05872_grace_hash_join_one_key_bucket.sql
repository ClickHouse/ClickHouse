-- Tags: no-random-settings
-- A bucket whose right side is one key can't be split, so it must not be repartitioned on every block.

SET query_plan_join_swap_table = 0, max_threads = 2, max_block_size = 65409, max_memory_usage = '400Mi',
    max_bytes_before_external_join = 0, grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1024;

SELECT count() FROM numbers(10) AS l INNER JOIN (SELECT toUInt64(0) AS k FROM numbers(8000000)) AS r ON l.number = r.k
SETTINGS join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0;

SELECT count() FROM numbers(10) AS l INNER JOIN (SELECT toUInt64(0) AS k FROM numbers(8000000)) AS r ON l.number = r.k
SETTINGS join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0.5;

SELECT count() FROM numbers(10) AS l LEFT JOIN (SELECT toUInt64(0) AS k FROM numbers(8000000)) AS r ON l.number = r.k
SETTINGS join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0.5;

SELECT count() FROM numbers(10) AS l INNER JOIN (SELECT toUInt64(0) AS k FROM numbers(8000000)) AS r ON l.number = r.k
SETTINGS join_algorithm = 'grace_hash', max_bytes_ratio_before_external_join = 0.5;

-- Two keys can still be split.
SELECT count() FROM numbers(10) AS l INNER JOIN (SELECT number % 2 AS k FROM numbers(8000000)) AS r ON l.number = r.k
SETTINGS join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0.5;
