-- A huge `preferred_block_size_bytes` produces a `SIZE_MAX` row budget in `MergeTreeReadTask::estimateNumRows`,
-- which must not wrap when added to the rows already read, both for adaptive and constant index granularity.

DROP TABLE IF EXISTS t_adaptive;
DROP TABLE IF EXISTS t_constant;

CREATE TABLE t_adaptive (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1, enable_index_granularity_compression = 0;

CREATE TABLE t_constant (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1, enable_index_granularity_compression = 1;

INSERT INTO t_adaptive VALUES (1), (2), (3);
INSERT INTO t_constant VALUES (1), (2), (3);

SELECT x FROM t_adaptive ORDER BY x
SETTINGS max_threads = 1, max_block_size = 1, preferred_block_size_bytes = 18446744073709551615,
    preferred_max_column_in_block_size_bytes = 0, use_columns_cache = 0, use_query_condition_cache = 0, enable_parallel_replicas = 0;

SELECT x FROM t_constant ORDER BY x
SETTINGS max_threads = 1, max_block_size = 1, preferred_block_size_bytes = 18446744073709551615,
    preferred_max_column_in_block_size_bytes = 0, use_columns_cache = 0, use_query_condition_cache = 0, enable_parallel_replicas = 0;

DROP TABLE t_adaptive;
DROP TABLE t_constant;
