-- A compact part has no sizes of individual columns. The first block read from a large compact part
-- must respect `preferred_block_size_bytes` for a column of variable size, and later blocks use its measured size.

DROP TABLE IF EXISTS t_block_size_compact;

CREATE TABLE t_block_size_compact (k UInt64, x UInt64, n String, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 256, index_granularity_bytes = '100Mi', min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_block_size_compact SELECT number, number, toString(number % 10), repeat('a', 2000) FROM numbers(20000);
OPTIMIZE TABLE t_block_size_compact FINAL;

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_block_size_compact' AND active;

-- `s` takes about 2000 bytes per row, so a block has about one granule of 256 rows, the first block included.
SELECT max(blockSize()) < 2048, sum(k), max(s) = repeat('a', 2000)
FROM t_block_size_compact
SETTINGS max_threads = 1, max_block_size = 8192, preferred_block_size_bytes = 200000;

-- `n` is narrow: once the first block is read, blocks are no longer limited to one granule.
SELECT max(blockSize()) > 2048, sum(toUInt64(n))
FROM t_block_size_compact
SETTINGS max_threads = 1, max_block_size = 8192, preferred_block_size_bytes = 200000;

DROP TABLE t_block_size_compact;
