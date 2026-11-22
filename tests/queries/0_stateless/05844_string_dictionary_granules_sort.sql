-- Sorting a `String` column with dictionary granules equals sorting its `with_size_stream` twin. The column
-- is sorted by rank as the only sort column and as the first of two, each with and without a limit; it is
-- sorted by comparison after a nearly unique key. The same values sit in the dictionaries of many granules.

DROP TABLE IF EXISTS t_sort_plain;
DROP TABLE IF EXISTS t_sort_dict;

CREATE TABLE t_sort_plain (id UInt64, g UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

CREATE TABLE t_sort_dict (id UInt64, g UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';

INSERT INTO t_sort_plain SELECT number, intDiv(number, 3), 'v' || toString(cityHash64(number) % 37) FROM numbers(30000);
INSERT INTO t_sort_dict SELECT number, intDiv(number, 3), 'v' || toString(cityHash64(number) % 37) FROM numbers(30000);

SET max_block_size = 500, max_threads = 1;

SELECT 'one column', (SELECT groupArray(s) FROM (SELECT s FROM t_sort_plain ORDER BY s))
    = (SELECT groupArray(s) FROM (SELECT s FROM t_sort_dict ORDER BY s));
SELECT 'one column, descending, limit', (SELECT groupArray(s) FROM (SELECT s FROM t_sort_plain ORDER BY s DESC LIMIT 7))
    = (SELECT groupArray(s) FROM (SELECT s FROM t_sort_dict ORDER BY s DESC LIMIT 7));
SELECT 'after large ties', (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_sort_plain ORDER BY s, id))
    = (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_sort_dict ORDER BY s, id));
SELECT 'after large ties, limit', (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_sort_plain ORDER BY s DESC, id LIMIT 10))
    = (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_sort_dict ORDER BY s DESC, id LIMIT 10));
SELECT 'after a nearly unique key', (SELECT groupArray((g, s, id)) FROM (SELECT g, s, id FROM t_sort_plain ORDER BY g, s, id))
    = (SELECT groupArray((g, s, id)) FROM (SELECT g, s, id FROM t_sort_dict ORDER BY g, s, id));
SELECT 'after a nearly unique key, limit', (SELECT groupArray((g, s, id)) FROM (SELECT g, s, id FROM t_sort_plain ORDER BY g DESC, s, id LIMIT 10))
    = (SELECT groupArray((g, s, id)) FROM (SELECT g, s, id FROM t_sort_dict ORDER BY g DESC, s, id LIMIT 10));

-- The threshold of a `LIMIT` of at least 1500 rows filters the blocks after the first. A block of 8000 rows
-- has about 3800 values. The threshold compares each value once when the block has at least as many rows as
-- values, and rows one by one when a filter left fewer, both for the first sort column and for the rows that
-- an earlier one leaves tied: `id % 1` ties every row of a block, `id % 1000` eight.
SET max_block_size = 8000;

SELECT 'threshold, by values', (SELECT groupArray(s) FROM (SELECT s FROM t_sort_plain ORDER BY s LIMIT 1500))
    = (SELECT groupArray(s) FROM (SELECT s FROM t_sort_dict ORDER BY s LIMIT 1500));
SELECT 'threshold, by values, after ties', (SELECT groupArray(s) FROM (SELECT s FROM t_sort_plain ORDER BY id % 1, s DESC LIMIT 1500))
    = (SELECT groupArray(s) FROM (SELECT s FROM t_sort_dict ORDER BY id % 1, s DESC LIMIT 1500));
SELECT 'threshold, by rows', (SELECT groupArray(s) FROM (SELECT s FROM t_sort_plain WHERE id % 4 = 0 ORDER BY s LIMIT 1500))
    = (SELECT groupArray(s) FROM (SELECT s FROM t_sort_dict WHERE id % 4 = 0 ORDER BY s LIMIT 1500));
SELECT 'threshold, by rows, after ties', (SELECT groupArray((id % 1000, s)) FROM (SELECT id, s FROM t_sort_plain ORDER BY id % 1000, s LIMIT 1500))
    = (SELECT groupArray((id % 1000, s)) FROM (SELECT id, s FROM t_sort_dict ORDER BY id % 1000, s LIMIT 1500));

DROP TABLE t_sort_plain;
DROP TABLE t_sort_dict;
