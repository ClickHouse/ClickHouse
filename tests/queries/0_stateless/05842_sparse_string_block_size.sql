-- Reads a sparse `String` column into blocks of a preferred size in bytes. The reading counts a sparse column at the
-- size it takes once expanded, so it reads about as many blocks as from the same data stored full.

DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_full;

-- At threshold 0 every column that holds a default value is sparse, at threshold 1 none is.
CREATE TABLE t_sparse (s String) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0, index_granularity = 1024, index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0;
CREATE TABLE t_full (s String) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1024, index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0;

-- One row of 8 has a value. Sparse, the column takes less than half of the bytes it takes expanded.
INSERT INTO t_sparse SELECT if(number % 8 = 0, repeat('x', 20), '') FROM numbers(100000);
INSERT INTO t_full SELECT * FROM t_sparse;

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse' AND active AND column = 's';

-- `sum(1 / blockSize())` counts the blocks.
SELECT
    (SELECT sum(1 / blockSize()) FROM t_sparse WHERE NOT ignore(s))
    / (SELECT sum(1 / blockSize()) FROM t_full WHERE NOT ignore(s)) BETWEEN 0.8 AND 1.25
SETTINGS max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 50000;

DROP TABLE t_sparse;
DROP TABLE t_full;
