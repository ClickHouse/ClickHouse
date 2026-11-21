-- Sorts by sparse columns with a `LIMIT`, which compares them with a threshold value through their values. Every case
-- compares the result with the same data stored full.

DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_full;

-- At threshold 0 every column that holds a default value is sparse, at threshold 1 none is.
CREATE TABLE t_sparse (n UInt64, g UInt8, s String) ENGINE = MergeTree ORDER BY n
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0, index_granularity = 64;
CREATE TABLE t_full (n UInt64, g UInt8, s String) ENGINE = MergeTree ORDER BY n
    SETTINGS ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 64;

INSERT INTO t_sparse SELECT number, number % 7, if(number % 3 = 0, toString(number % 997), '') FROM numbers(10000);
INSERT INTO t_full SELECT * FROM t_sparse;

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse' AND active AND column IN ('g', 's');

-- One stream reads blocks of 2048 rows.
SET max_threads = 1, max_block_size = 2000;

-- The top-K filter compares the column with the threshold value of the sorting while the table is read, every row.
SELECT 'one column',
    (SELECT groupArray(s) FROM (SELECT s FROM t_sparse ORDER BY s DESC LIMIT 10))
    = (SELECT groupArray(s) FROM (SELECT s FROM t_full ORDER BY s DESC LIMIT 10))
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000;

-- Without the top-K filter and with a `LIMIT` of at least 1500, the sorting takes a threshold row from the first block
-- and compares the next blocks with it. With several sort columns, a column compares the rows that tie on the columns
-- before it, and the first column all rows: here `g` all rows, then `s` the ties of `g`.
SELECT 'several columns',
    (SELECT groupArray((g, s, n)) FROM (SELECT g, s, n FROM t_sparse ORDER BY g, s DESC, n LIMIT 1500))
    = (SELECT groupArray((g, s, n)) FROM (SELECT g, s, n FROM t_full ORDER BY g, s DESC, n LIMIT 1500))
SETTINGS use_top_k_dynamic_filtering = 0;

DROP TABLE t_sparse;
DROP TABLE t_full;
