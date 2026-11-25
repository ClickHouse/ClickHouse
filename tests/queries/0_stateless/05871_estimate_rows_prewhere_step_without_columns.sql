-- Disable force_primary_key_reverse_order: the test relies on the layout of granules of an ascending key (the first granule has fewer rows than the next ones)
SET force_primary_key_reverse_order = 0;

-- The size of the next read is estimated from the position of the readers chain.
-- The last PREWHERE step here reads no columns (`a + b > 11` needs only columns read by earlier steps),
-- so its stream never moves from mark 0. The estimate took the mark from that step and the offset in the granule
-- from the first step: once the first step was inside the second granule, further than the rows of the first granule,
-- `countRowsForRows` wrapped around (an UBSan report in builds with the unsigned overflow check),
-- and the read took `max_block_size` rows instead of about `preferred_max_column_in_block_size_bytes`.

DROP TABLE IF EXISTS t_estimate_rows;

-- The granules follow the size of rows: the first one has fewer rows than the next ones.
CREATE TABLE t_estimate_rows (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 16384, index_granularity_bytes = '1Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192, ratio_of_defaults_for_sparse_serialization = 1,
    use_const_adaptive_granularity = 0;

INSERT INTO t_estimate_rows SELECT number, number, repeat('x', 100) FROM numbers(8000);
INSERT INTO t_estimate_rows SELECT 8000 + number, number, '' FROM numbers(100000);
OPTIMIZE TABLE t_estimate_rows FINAL;

SELECT 'layout', rows_in_granule[1] < rows_in_granule[2]
FROM (SELECT groupArray(rows_in_granule) AS rows_in_granule FROM mergeTreeIndex(currentDatabase(), t_estimate_rows));

-- The number of rows (all of them pass) and the number of blocks above the ~1000 rows that `preferred_max_column_in_block_size_bytes` allows.
SELECT 'multiple steps', sum(c), countIf(bs > 2000) FROM
(
    SELECT blockSize() AS bs, count() AS c FROM t_estimate_rows PREWHERE a > 5 AND b > 5 AND a + b > 11 GROUP BY blockNumber(), bs
)
SETTINGS enable_multiple_prewhere_read_steps = 1, max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 1000000,
    preferred_max_column_in_block_size_bytes = 8000, use_query_condition_cache = 0, use_columns_cache = 0, enable_parallel_replicas = 0;

SELECT 'one step', sum(c), countIf(bs > 2000) FROM
(
    SELECT blockSize() AS bs, count() AS c FROM t_estimate_rows PREWHERE a > 5 AND b > 5 AND a + b > 11 GROUP BY blockNumber(), bs
)
SETTINGS enable_multiple_prewhere_read_steps = 0, max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 1000000,
    preferred_max_column_in_block_size_bytes = 8000, use_query_condition_cache = 0, use_columns_cache = 0, enable_parallel_replicas = 0;

DROP TABLE t_estimate_rows;
