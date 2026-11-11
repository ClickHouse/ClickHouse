-- A read of a sparse column in a wide part that starts in a granule without values and continues
-- into a granule with values must read the values of that granule.

DROP TABLE IF EXISTS t_sparse_wide_range;
DROP TABLE IF EXISTS t_dense_wide_range;

CREATE TABLE t_sparse_wide_range (k UInt64, u UInt32, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, index_granularity = 64, index_granularity_bytes = '10M';

CREATE TABLE t_dense_wide_range (k UInt64, u UInt32, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 64, index_granularity_bytes = '10M';

-- Values only in granules 0 and 2.
INSERT INTO t_sparse_wide_range SELECT number, if(number IN (1, 2, 130, 131), number + 1, 0), if(number IN (1, 2, 130, 131), toString(number), '') FROM numbers(1000);
INSERT INTO t_dense_wide_range SELECT * FROM t_sparse_wide_range;

SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_wide_range' AND active AND column != 'k'
ORDER BY column;

-- The read of the values starts at granule 1, which has none, and continues into granule 2.
-- Prefetching would open and position the streams of the values in advance, and the columns cache reads whole
-- stripes of granules, so both are disabled.
SELECT
    (SELECT sum(cityHash64(k, u, s)) FROM t_sparse_wide_range PREWHERE k BETWEEN 64 AND 191
        SETTINGS max_block_size = 16, max_threads = 1, local_filesystem_read_prefetch = 0, use_columns_cache = 0)
    = (SELECT sum(cityHash64(k, u, s)) FROM t_dense_wide_range PREWHERE k BETWEEN 64 AND 191
        SETTINGS max_block_size = 16, max_threads = 1, local_filesystem_read_prefetch = 0, use_columns_cache = 0);

SELECT k, u, s FROM t_sparse_wide_range PREWHERE k BETWEEN 64 AND 191 WHERE u != 0 ORDER BY k
SETTINGS max_block_size = 16, max_threads = 1, local_filesystem_read_prefetch = 0, use_columns_cache = 0;

SELECT sum(u), arraySort(groupUniqArray(s)) FROM t_sparse_wide_range PREWHERE k BETWEEN 64 AND 191
SETTINGS max_block_size = 16, max_threads = 1, local_filesystem_read_prefetch = 0, use_columns_cache = 0;

DROP TABLE t_sparse_wide_range;
DROP TABLE t_dense_wide_range;
