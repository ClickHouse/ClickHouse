-- Several build streams can each store a block below `join_in_memory_compression_min_bytes` and cross it together,
-- before any of them decides to compress. A build stream that then finds `max_bytes_in_join` exceeded must make
-- that decision and compress the whole join before reporting the excess: the join must not fail with
-- `SET_SIZE_LIMIT_EXCEEDED` when the whole right side, compressed, fits under the limit.

DROP TABLE IF EXISTS jimc_ca;

CREATE TABLE jimc_ca (k UInt64, pad String) ENGINE = Memory;

-- Eight blocks of about 3 MB, highly compressible.
INSERT INTO jimc_ca SELECT number, repeat(toString(number % 10), 1000) FROM numbers(24000)
SETTINGS max_block_size = 3000, min_insert_block_size_rows = 3000;

-- The build streams cross the threshold concurrently only sometimes, so the query runs a few times.
SELECT count(), sum(cityHash64(l.number, r.pad)) != 0
FROM numbers(24000) AS l INNER JOIN jimc_ca AS r ON l.number = r.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel',
    max_threads = 8, max_block_size = 3000,
    enable_join_in_memory_compression = 1,
    join_in_memory_compression_min_bytes = 18000000,
    max_bytes_in_join = 22000000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

SELECT count(), sum(cityHash64(l.number, r.pad)) != 0
FROM numbers(24000) AS l INNER JOIN jimc_ca AS r ON l.number = r.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel',
    max_threads = 8, max_block_size = 3000,
    enable_join_in_memory_compression = 1,
    join_in_memory_compression_min_bytes = 18000000,
    max_bytes_in_join = 22000000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

SELECT count(), sum(cityHash64(l.number, r.pad)) != 0
FROM numbers(24000) AS l INNER JOIN jimc_ca AS r ON l.number = r.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel',
    max_threads = 8, max_block_size = 3000,
    enable_join_in_memory_compression = 1,
    join_in_memory_compression_min_bytes = 18000000,
    max_bytes_in_join = 22000000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

SELECT count(), sum(cityHash64(l.number, r.pad)) != 0
FROM numbers(24000) AS l INNER JOIN jimc_ca AS r ON l.number = r.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel',
    max_threads = 8, max_block_size = 3000,
    enable_join_in_memory_compression = 1,
    join_in_memory_compression_min_bytes = 18000000,
    max_bytes_in_join = 22000000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

SELECT count(), sum(cityHash64(l.number, r.pad)) != 0
FROM numbers(24000) AS l INNER JOIN jimc_ca AS r ON l.number = r.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel',
    max_threads = 8, max_block_size = 3000,
    enable_join_in_memory_compression = 1,
    join_in_memory_compression_min_bytes = 18000000,
    max_bytes_in_join = 22000000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

DROP TABLE jimc_ca;
