-- Tags: no-fasttest
-- Tag no-fasttest: collations need ICU.

-- Sorting a `String` column with dictionary granules with a collation equals sorting its `with_size_stream`
-- twin. The column is sorted by rank as the first of two sort columns, with and without a limit, and as the
-- only one with a limit, and by comparison after a nearly unique key. The values differ in case, so that the
-- collation orders them unlike their bytes.

DROP TABLE IF EXISTS t_collation_plain;
DROP TABLE IF EXISTS t_collation_dict;

CREATE TABLE t_collation_plain (id UInt64, g UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

CREATE TABLE t_collation_dict (id UInt64, g UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';

INSERT INTO t_collation_plain SELECT number, intDiv(number, 3), ['a', 'B', 'b', 'A', 'ab', 'Ab', 'c'][number % 7 + 1] FROM numbers(2000);
INSERT INTO t_collation_dict SELECT number, intDiv(number, 3), ['a', 'B', 'b', 'A', 'ab', 'Ab', 'c'][number % 7 + 1] FROM numbers(2000);

SET max_block_size = 500, max_threads = 1;

SELECT 'one column', (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_collation_plain ORDER BY s COLLATE 'en', id))
    = (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_collation_dict ORDER BY s COLLATE 'en', id));
SELECT 'one column, then id, limit', (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_collation_plain ORDER BY s COLLATE 'en', id LIMIT 10))
    = (SELECT groupArray((s, id)) FROM (SELECT s, id FROM t_collation_dict ORDER BY s COLLATE 'en', id LIMIT 10));
SELECT 'one column, limit', (SELECT groupArray(lower(s)) FROM (SELECT s FROM t_collation_plain ORDER BY s DESC COLLATE 'en' LIMIT 5))
    = (SELECT groupArray(lower(s)) FROM (SELECT s FROM t_collation_dict ORDER BY s DESC COLLATE 'en' LIMIT 5));
SELECT 'after a nearly unique key', (SELECT groupArray((g, s, id)) FROM (SELECT g, s, id FROM t_collation_plain ORDER BY g, s COLLATE 'en', id))
    = (SELECT groupArray((g, s, id)) FROM (SELECT g, s, id FROM t_collation_dict ORDER BY g, s COLLATE 'en', id));

DROP TABLE t_collation_plain;
DROP TABLE t_collation_dict;
