-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN indexes = 1 gains a per-node Granules block.

-- Bulk filtering of a `set` index must bound a batch by the number of stored values, not by the number
-- of index granules. The part has 733 index granules with 8192 distinct values each, about 92 MiB of
-- values and granule numbers in total. A batch of about 65k values takes about 1 MiB, so a lookup of
-- an absent value fits into a small memory limit. Reading `max_block_size` granules into one batch
-- would read the whole index at once and exceed it.

SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_set_bulk_memory;
CREATE TABLE t_set_bulk_memory (v UInt64, INDEX idx v TYPE set(10000) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_set_bulk_memory SELECT number * 2 FROM numbers(6000000);
OPTIMIZE TABLE t_set_bulk_memory FINAL;

SELECT count() FROM t_set_bulk_memory WHERE v = 1000001
SETTINGS secondary_indices_enable_bulk_filtering = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0,
    max_threads = 1, max_block_size = 65409, max_memory_usage = '32M';

-- The same holds when bulk filtering of minmax indexes is enabled: the setting does not affect `set` indexes.
SELECT count() FROM t_set_bulk_memory WHERE v = 1000001
SETTINGS secondary_indices_enable_bulk_filtering = 1, use_minmax_index_bulk_filtering = 1, use_skip_indexes_on_data_read = 0,
    use_query_condition_cache = 0, max_threads = 1, max_block_size = 65409, max_memory_usage = '32M';

-- The index is evaluated over the whole part and prunes every granule.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_bulk_memory WHERE v = 1000001
    SETTINGS secondary_indices_enable_bulk_filtering = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0)
WHERE explain LIKE '%Granules: 0/733%';

-- A present value is still found.
SELECT count() FROM t_set_bulk_memory WHERE v = 1000000
SETTINGS secondary_indices_enable_bulk_filtering = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

DROP TABLE t_set_bulk_memory;
