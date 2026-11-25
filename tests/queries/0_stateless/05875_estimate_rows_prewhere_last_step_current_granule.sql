-- The last PREWHERE step here reads no columns, so its stream never moves.
-- The next read was sized by the first granule of the task instead of the current one.

DROP TABLE IF EXISTS t_estimate_rows_last_step;

-- The first granule has 8192 small rows, the next ones about 100 wide rows.
CREATE TABLE t_estimate_rows_last_step (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '100Ki', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192, ratio_of_defaults_for_sparse_serialization = 1,
    use_const_adaptive_granularity = 0;

INSERT INTO t_estimate_rows_last_step SELECT number, '' FROM numbers(8192);
INSERT INTO t_estimate_rows_last_step SELECT 8192 + number, repeat('x', 1000) FROM numbers(8192);
OPTIMIZE TABLE t_estimate_rows_last_step FINAL;

SELECT 'layout', rows_in_granule[1], rows_in_granule[2] < 1000
FROM (SELECT groupArray(rows_in_granule) AS rows_in_granule FROM mergeTreeIndex(currentDatabase(), t_estimate_rows_last_step));

-- Only the block of the first granule may have more than 1000 rows.
SELECT 'multiple steps', sum(c), countIf(bs > 1000) FROM
(
    SELECT blockSize() AS bs, count() AS c FROM t_estimate_rows_last_step PREWHERE s != 'z' AND id > 5 AND concat(s, toString(id)) != 'z' GROUP BY blockNumber(), bs
)
SETTINGS enable_multiple_prewhere_read_steps = 1, max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 100000,
    preferred_max_column_in_block_size_bytes = 0, use_query_condition_cache = 0, use_columns_cache = 0, enable_parallel_replicas = 0,
    allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;

SELECT 'one step', sum(c), countIf(bs > 1000) FROM
(
    SELECT blockSize() AS bs, count() AS c FROM t_estimate_rows_last_step PREWHERE s != 'z' AND id > 5 AND concat(s, toString(id)) != 'z' GROUP BY blockNumber(), bs
)
SETTINGS enable_multiple_prewhere_read_steps = 0, max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 100000,
    preferred_max_column_in_block_size_bytes = 0, use_query_condition_cache = 0, use_columns_cache = 0, enable_parallel_replicas = 0,
    allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;

DROP TABLE t_estimate_rows_last_step;
