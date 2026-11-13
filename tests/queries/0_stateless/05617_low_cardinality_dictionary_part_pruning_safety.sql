-- Pruning by the dictionaries of `LowCardinality` columns must not change the result of a query or make it fail.

SET use_low_cardinality_dictionaries_for_part_pruning = 1;
SET optimize_use_implicit_projections = 0;
SET min_marks_per_part_to_load_statistics = 0;

-- Functions that depend on the block or the row are not evaluated on the dictionary values.
DROP TABLE IF EXISTS t_lc_nondeterministic;
CREATE TABLE t_lc_nondeterministic (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
INSERT INTO t_lc_nondeterministic SELECT number, 'a' FROM numbers(10000);
SELECT 'blockSize', count() FROM t_lc_nondeterministic WHERE s = 'absent' OR blockSize() > 10 SETTINGS optimize_move_to_prewhere = 0, max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000;
SELECT 'rowNumberInBlock', count() FROM t_lc_nondeterministic WHERE s = 'absent' OR (rowNumberInBlock() = 10 AND id < 100) SETTINGS optimize_move_to_prewhere = 0, max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000;
DROP TABLE t_lc_nondeterministic;

-- The filter is evaluated on the values of the rows skipped by the primary key, so it must not throw on them.
DROP TABLE IF EXISTS t_lc_throw;
CREATE TABLE t_lc_throw (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_throw;
INSERT INTO t_lc_throw VALUES (0, 'a');
INSERT INTO t_lc_throw VALUES (100, '[');
INSERT INTO t_lc_throw VALUES (200, '1');
INSERT INTO t_lc_throw VALUES (300, 'invalid');
-- The column is the pattern of `match`, which is invalid in a skipped part.
SELECT 'match pattern', count() FROM t_lc_throw WHERE id = 0 AND match('a', s);
-- A conversion inside `indexHint` throws on a value of a skipped part.
SELECT 'indexHint', count() FROM t_lc_throw WHERE id = 200 AND s != 'absent' AND indexHint(toUInt32(s) = 1);
-- The column is the haystack of `match`, which is safe, so the parts are still skipped.
SELECT 'match haystack', count() FROM t_lc_throw WHERE match(s, '^inv');
SELECT 'match haystack', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_throw WHERE match(s, '^inv'));
DROP TABLE t_lc_throw;

-- A merged dictionary can have blocks compressed with different codecs.
DROP TABLE IF EXISTS t_lc_codecs;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_lc_codecs (id UInt64, s LowCardinality(UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1,
    enable_adaptive_codec_selection = 1, adaptive_codec_selection_period = 1, min_compress_block_size = 1024, max_compress_block_size = 1024;
INSERT INTO t_lc_codecs SELECT number, number FROM numbers(4000);
INSERT INTO t_lc_codecs SELECT number, cityHash64(number) FROM numbers(4000, 4000);
OPTIMIZE TABLE t_lc_codecs FINAL;
SELECT 'mixed codecs', count() FROM t_lc_codecs WHERE s = 100;
SELECT 'mixed codecs', count() FROM t_lc_codecs WHERE s = 100 SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 0;
DROP TABLE t_lc_codecs;
