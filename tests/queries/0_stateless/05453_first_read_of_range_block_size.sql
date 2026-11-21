-- The first read of a range must take its size from the first granule of that range,
-- not from the granule at the reader's previous position: mark 0 for a new task,
-- or the end of the previous range. Here small rows (8192 per granule) come before big rows
-- (~100 per granule). Before the fix, the first read of a range of big rows took all of its 1500 rows at once.

DROP TABLE IF EXISTS t_first_read_block_size;

-- Granules must follow row sizes: ~8192 small rows or ~100 big rows each. A vertical merge would size them by `id` only.
CREATE TABLE t_first_read_block_size (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '1M', use_const_adaptive_granularity = 0,
    enable_vertical_merge_algorithm = 0, min_bytes_for_wide_part = 0;

-- 49152 small rows (~100 bytes, 6 full granules), then 2000 big rows (~9.4 KB, ~100 rows per granule).
INSERT INTO t_first_read_block_size
SELECT number, concat(toString(number), if(number < 49152, repeat('n', 100), repeat('w', 9400)))
FROM numbers(51152);

OPTIMIZE TABLE t_first_read_block_size FINAL;

-- Verify that reading the big-row range does not produce oversized blocks.
-- The filter keeps every row and needs every string. `count` checks that no rows were skipped.
-- Keep `s` in the aggregation so the size predictor accounts for it.

-- One range that starts among the big rows: the reader is at mark 0 before the first read.
SELECT 'one_range', count(), max(length(s)) > 0 AND max(blockSize()) < 1000 ? 'OK' : format('FAIL: block of {} rows', max(blockSize()))
FROM t_first_read_block_size
WHERE id >= 49652 AND cityHash64(s) != 1
SETTINGS max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 100000,
    preferred_max_column_in_block_size_bytes = 0, optimize_move_to_prewhere = 0, enable_parallel_replicas = 0;

-- Two ranges: small rows, then big rows. Before the second range, the reader is at the end of the first one.
SELECT 'two_ranges', count(), max(length(s)) > 0 AND maxIf(blockSize(), id >= 49652) < 1000 ? 'OK' : format('FAIL: block of {} rows', maxIf(blockSize(), id >= 49652))
FROM t_first_read_block_size
WHERE (id < 8192 OR id >= 49652) AND cityHash64(s) != 1
SETTINGS max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 100000,
    preferred_max_column_in_block_size_bytes = 0, optimize_move_to_prewhere = 0, enable_parallel_replicas = 0;

DROP TABLE t_first_read_block_size;
