-- The copies a read makes to populate the columns cache belong to the shared cache, not to the query:
-- they are not charged to the memory limit of the query. A streaming read that fits the limit without
-- the cache fits it with the cache as well, although a stripe of the column is larger than the limit.

DROP TABLE IF EXISTS t_columns_cache_memory;

CREATE TABLE t_columns_cache_memory (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 70000 rows of 1 KiB: a stripe of 65536 rows is 64 MiB, more than the limit below.
INSERT INTO t_columns_cache_memory SELECT number, repeat('x', 1024) FROM numbers(70000);

SELECT count(), sum(length(s)) FROM t_columns_cache_memory
SETTINGS use_columns_cache = 0, max_threads = 1, max_block_size = 1024, use_uncompressed_cache = 0,
    enable_parallel_replicas = 0, max_memory_usage = 50000000;

SELECT count(), sum(length(s)) FROM t_columns_cache_memory
SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
    columns_cache_max_estimated_bytes_to_write_to_cache = 10000000000, columns_cache_max_bytes_to_write_to_cache = 10000000000,
    max_threads = 1, max_block_size = 1024, use_uncompressed_cache = 0, enable_parallel_replicas = 0, max_memory_usage = 50000000;

DROP TABLE t_columns_cache_memory;
