DROP TABLE IF EXISTS t_prewhere_block_size;
CREATE TABLE t_prewhere_block_size (k UInt64, x UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_prewhere_block_size SELECT number, number, repeat('a', 1000) FROM numbers(10000);

-- Two PREWHERE steps; nothing they read is needed after them.
SELECT max(blockSize()) <= 1024 FROM t_prewhere_block_size WHERE x != 12345678 AND s LIKE '%a%'
SETTINGS max_threads = 1, preferred_block_size_bytes = 100000, enable_multiple_prewhere_read_steps = 1, optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1;

-- One PREWHERE step; only a narrow column is needed after it.
SELECT max(blockSize()) <= 1024, sum(k) FROM t_prewhere_block_size WHERE s LIKE '%a%'
SETTINGS max_threads = 1, preferred_block_size_bytes = 100000, optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1;

DROP TABLE t_prewhere_block_size;
