-- With `join_in_memory_compression_min_bytes` above zero, one build stream can store its only block uncompressed
-- before another one crosses the threshold. The size limit must see that block compressed too: the join must not
-- fail with `SET_SIZE_LIMIT_EXCEEDED` when the whole right side, compressed, fits under `max_bytes_in_join`.

DROP TABLE IF EXISTS jimc_lp_a;
DROP TABLE IF EXISTS jimc_lp_b;

CREATE TABLE jimc_lp_a (k UInt64, pad String) ENGINE = Memory;
CREATE TABLE jimc_lp_b (k UInt64, pad String) ENGINE = Memory;

-- About 3 MB in one block for each build stream; each string repeats itself, so it compresses about twofold.
INSERT INTO jimc_lp_a SELECT number, concat(s, s) FROM (SELECT number, randomString(500) AS s FROM numbers(3000))
SETTINGS max_block_size = 3000, min_insert_block_size_rows = 3000;
INSERT INTO jimc_lp_b SELECT number + 3000, concat(s, s) FROM (SELECT number, randomString(500) AS s FROM numbers(3000))
SETTINGS max_block_size = 3000, min_insert_block_size_rows = 3000;

SELECT count(), sum(cityHash64(l.number, r.pad)) != 0
FROM numbers(6000) AS l
INNER JOIN (SELECT k, pad FROM jimc_lp_a UNION ALL SELECT k, pad FROM jimc_lp_b) AS r
ON l.number = r.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel',
    max_threads = 2, max_block_size = 3000,
    enable_join_in_memory_compression = 1,
    join_in_memory_compression_min_bytes = 5000000,
    max_bytes_in_join = 5100000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

DROP TABLE jimc_lp_a;
DROP TABLE jimc_lp_b;
