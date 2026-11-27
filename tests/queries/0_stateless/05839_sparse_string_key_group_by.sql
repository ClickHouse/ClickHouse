-- Groups by a sparse `String` key, which the methods of one `String` key and of fixed-width keys and one `String` read
-- in place through the positions of its values. Every case compares the result with the same data stored full.

-- Aggregation in order reads the keys elsewhere.
SET optimize_aggregation_in_order = 0;

DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_full;

-- At threshold 0 every column that holds a default value is sparse, at threshold 1 none is.
CREATE TABLE t_sparse (k UInt64, b UInt8, s String) ENGINE = MergeTree ORDER BY k
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0, index_granularity = 64;
CREATE TABLE t_full (k UInt64, b UInt8, s String) ENGINE = MergeTree ORDER BY k
    SETTINGS ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 64;

-- Two rows of three have a value in `s`. So the 140000 rows of the part have more values than `UInt16` indexes reach.
INSERT INTO t_sparse SELECT number % 1000, number % 5, if(number % 3 != 0, toString(number % 7919), '') FROM numbers(140000)
    SETTINGS max_insert_threads = 1;
INSERT INTO t_full SELECT * FROM t_sparse;

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse' AND active AND column = 's';

SELECT 'one key',
    (SELECT sum(cityHash64(*)) FROM (SELECT s, count(), sum(b) FROM t_sparse GROUP BY s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT s, count(), sum(b) FROM t_full GROUP BY s));

SELECT 'UInt64 and String',
    (SELECT sum(cityHash64(*)) FROM (SELECT k, s, count() FROM t_sparse GROUP BY k, s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT k, s, count() FROM t_full GROUP BY k, s));

-- Without aggregate functions, the method is the key-only form.
SELECT 'UInt64 and String, no aggregate functions',
    (SELECT sum(cityHash64(*)) FROM (SELECT k, s FROM t_sparse GROUP BY k, s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT k, s FROM t_full GROUP BY k, s));

SELECT 'UInt8 and String',
    (SELECT sum(cityHash64(*)) FROM (SELECT b, s, count() FROM t_sparse GROUP BY b, s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT b, s, count() FROM t_full GROUP BY b, s));

SELECT 'UInt64, UInt8 and String',
    (SELECT sum(cityHash64(*)) FROM (SELECT k, b, s, count() FROM t_sparse GROUP BY k, b, s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT k, b, s, count() FROM t_full GROUP BY k, b, s));

-- Blocks of 64 rows get indexes of one byte, a block of all 140000 rows gets indexes of four bytes.
SELECT 'indexes of one byte',
    (SELECT sum(cityHash64(*)) FROM (SELECT k, s, count() FROM t_sparse GROUP BY k, s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT k, s, count() FROM t_full GROUP BY k, s))
SETTINGS max_threads = 1, max_block_size = 64, preferred_block_size_bytes = 1000000000;

SELECT 'indexes of four bytes',
    (SELECT sum(cityHash64(*)) FROM (SELECT s, count() FROM t_sparse GROUP BY s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT s, count() FROM t_full GROUP BY s))
SETTINGS max_threads = 1, max_block_size = 140000, preferred_block_size_bytes = 1000000000;

-- Four streams read the part. The adaptive aggregator freezes their tables after 128 keys, probes them and stages the
-- other keys.
SELECT 'adaptive aggregator, frozen',
    (SELECT sum(cityHash64(*)) FROM (SELECT k, s, count() FROM t_sparse GROUP BY k, s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT k, s, count() FROM t_full GROUP BY k, s))
SETTINGS enable_adaptive_aggregator = 1, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    adaptive_aggregator_freeze_threshold = 128, group_by_two_level_threshold = 10000000,
    group_by_two_level_threshold_bytes = 500000000, collect_hash_table_stats_during_aggregation = 0;

SELECT 'two-level',
    (SELECT sum(cityHash64(*)) FROM (SELECT s, count() FROM t_sparse GROUP BY s))
    = (SELECT sum(cityHash64(*)) FROM (SELECT s, count() FROM t_full GROUP BY s))
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

-- An active top-K heap builds its ranked columns from the key columns, so the key is expanded.
SELECT 'top-K heap',
    (SELECT groupArray((k, s, c)) FROM (SELECT k, s, count() AS c FROM t_sparse GROUP BY k, s ORDER BY k, s LIMIT 10))
    = (SELECT groupArray((k, s, c)) FROM (SELECT k, s, count() AS c FROM t_full GROUP BY k, s ORDER BY k, s LIMIT 10));

DROP TABLE t_sparse;
DROP TABLE t_full;
