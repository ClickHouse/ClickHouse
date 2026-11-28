-- distinctJSONPaths read through the special subcolumn (optimize_functions_to_subcolumns = 1) must not return a dynamic
-- path that has no non-null value left in the table, same as the aggregate over the full column (= 0).

DROP TABLE IF EXISTS replacing_wide;
DROP TABLE IF EXISTS delete_wide;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS filtered_wide;
DROP TABLE IF EXISTS update_wide;
DROP TABLE IF EXISTS source_parts;
DROP TABLE IF EXISTS copy_compact;
DROP TABLE IF EXISTS replacing_compact_no_substream_marks;
DROP TABLE IF EXISTS memory_copy;

CREATE TABLE replacing_wide (key UInt64, j JSON) ENGINE = ReplacingMergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO replacing_wide VALUES (1, '{"a":1}');
INSERT INTO replacing_wide VALUES (1, '{"b":2}');
OPTIMIZE TABLE replacing_wide FINAL;
SELECT 'replacing wide', distinctJSONPaths(j) FROM replacing_wide SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'replacing wide', distinctJSONPaths(j) FROM replacing_wide SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'rewrite applied', count() FROM (EXPLAIN QUERY TREE SELECT distinctJSONPaths(j) FROM replacing_wide SETTINGS optimize_functions_to_subcolumns = 1)
WHERE explain ILIKE '%__special_subcolumn_name_for_distinct_paths_calculation%';

CREATE TABLE delete_wide (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO delete_wide VALUES (1, '{"a":1}'), (2, '{"b":2}');
ALTER TABLE delete_wide DELETE WHERE key = 1 SETTINGS mutations_sync = 2;
SELECT 'alter delete wide', distinctJSONPaths(j) FROM delete_wide SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'alter delete wide', distinctJSONPaths(j) FROM delete_wide SETTINGS optimize_functions_to_subcolumns = 0;

CREATE TABLE source (key UInt64, j JSON) ENGINE = Memory;
INSERT INTO source VALUES (1, '{"a":1}'), (2, '{"b":2}');
CREATE TABLE filtered_wide (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO filtered_wide SELECT * FROM source WHERE key = 2;
SELECT 'filtered insert wide', distinctJSONPaths(j) FROM filtered_wide SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'filtered insert wide', distinctJSONPaths(j) FROM filtered_wide SETTINGS optimize_functions_to_subcolumns = 0;

-- Path p has values only in rows >= 10000, so a statistic taken from the first 8192-row block alone would miss it.
CREATE TABLE update_wide (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, merge_max_block_size = 8192;
INSERT INTO update_wide SELECT number, if(number < 10000, '{"a":1}', '{"a":1,"p":1}') FROM numbers(20000);
ALTER TABLE update_wide UPDATE j = '{"z":1}' WHERE key = 1 SETTINGS mutations_sync = 2;
SELECT 'multi-block update wide', distinctJSONPaths(j) FROM update_wide SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'multi-block update wide', distinctJSONPaths(j) FROM update_wide SETTINGS optimize_functions_to_subcolumns = 0;

-- The Compact part keeps the statistics of the first source part (p:0), although the inserted block has a value of p.
CREATE TABLE source_parts (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO source_parts VALUES (1, '{"a":1}'), (2, '{"p":1}');
ALTER TABLE source_parts DELETE WHERE key = 2 SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES source_parts;
INSERT INTO source_parts VALUES (3, '{"p":2}');
CREATE TABLE copy_compact (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000, write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO copy_compact SELECT * FROM source_parts SETTINGS max_threads = 1, max_insert_threads = 1;
SELECT 'insert select compact', distinctJSONPaths(j) FROM copy_compact SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'insert select compact', distinctJSONPaths(j) FROM copy_compact SETTINGS optimize_functions_to_subcolumns = 0;

CREATE TABLE replacing_compact_no_substream_marks (key UInt64, j JSON) ENGINE = ReplacingMergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000, write_marks_for_substreams_in_compact_parts = 0;
INSERT INTO replacing_compact_no_substream_marks VALUES (1, '{"a":1}');
INSERT INTO replacing_compact_no_substream_marks VALUES (1, '{"b":2}');
OPTIMIZE TABLE replacing_compact_no_substream_marks FINAL;
SELECT 'replacing compact without substream marks', distinctJSONPaths(j) FROM replacing_compact_no_substream_marks SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'replacing compact without substream marks', distinctJSONPaths(j) FROM replacing_compact_no_substream_marks SETTINGS optimize_functions_to_subcolumns = 0;

CREATE TABLE memory_copy (key UInt64, j JSON) ENGINE = Memory;
INSERT INTO memory_copy SELECT * FROM replacing_wide;
SELECT 'memory copy', distinctJSONPaths(j) FROM memory_copy SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'memory copy', distinctJSONPaths(j) FROM memory_copy SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE replacing_wide;
DROP TABLE delete_wide;
DROP TABLE source;
DROP TABLE filtered_wide;
DROP TABLE update_wide;
DROP TABLE source_parts;
DROP TABLE copy_compact;
DROP TABLE replacing_compact_no_substream_marks;
DROP TABLE memory_copy;
