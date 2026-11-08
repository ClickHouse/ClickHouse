-- Parts are skipped by the dictionaries of `LowCardinality` columns.
-- Each query prints the result and the number of parts left to read after the index analysis.

SET use_low_cardinality_dictionaries_for_part_pruning = 1;
SET max_low_cardinality_dictionaries_bytes_for_part_pruning = 1048576;
SET optimize_use_implicit_projections = 0;
-- The parts are small, so allow reading the dictionaries for any number of marks to read.
SET min_marks_per_part_to_load_statistics = 0;

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;

SYSTEM STOP MERGES t_lc;
INSERT INTO t_lc SELECT number, ['a', 'b'][number % 2 + 1] FROM numbers(100);
INSERT INTO t_lc SELECT number + 100, ['c', 'd'][number % 2 + 1] FROM numbers(100);
INSERT INTO t_lc SELECT number + 200, 'e' FROM numbers(100);

SELECT 'equals', count() FROM t_lc WHERE s = 'c';
SELECT 'equals', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'c');
SELECT 'in', count() FROM t_lc WHERE s IN ('a', 'e');
SELECT 'in', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s IN ('a', 'e'));
SELECT 'like', count() FROM t_lc WHERE s LIKE 'd%';
SELECT 'like', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s LIKE 'd%');
SELECT 'no match', count() FROM t_lc WHERE s = 'zzz';
SELECT 'no match', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'zzz');
SELECT 'and', count() FROM t_lc WHERE s = 'c' AND id >= 100;
SELECT 'and', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'c' AND id >= 100);

-- The dictionaries always contain the default value, so they cannot skip parts for it.
SELECT 'not equals', count() FROM t_lc WHERE s != 'a';
SELECT 'not equals', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s != 'a');
SELECT 'default', count() FROM t_lc WHERE s = '';
SELECT 'default', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = '');
SELECT 'or other column', count() FROM t_lc WHERE s = 'c' OR id = 5;
SELECT 'or other column', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'c' OR id = 5);

SELECT 'disabled', count() FROM t_lc WHERE s = 'c' SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 0;
SELECT 'disabled', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'c') SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 0;
SELECT 'too large', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'c') SETTINGS max_low_cardinality_dictionaries_bytes_for_part_pruning = 1;

-- A pending mutation of the column is applied on the fly, so the dictionaries do not describe the values a query sees.
ALTER TABLE t_lc UPDATE s = 'zzz' WHERE id = 0 SETTINGS mutations_sync = 0;
SELECT 'mutation on the fly', count() FROM t_lc WHERE s = 'zzz' SETTINGS apply_mutations_on_fly = 1;
SELECT 'mutation on the fly', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc WHERE s = 'zzz') SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lc;

-- A merged part has the values of all merged parts in its dictionary.
DROP TABLE IF EXISTS t_lc_merge;
CREATE TABLE t_lc_merge (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id PARTITION BY id % 2
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
INSERT INTO t_lc_merge SELECT number, 'p' || toString(number % 2) || '_' || toString(number % 5) FROM numbers(100);
INSERT INTO t_lc_merge SELECT number + 100, 'p' || toString(number % 2) || '_' || toString(number % 5 + 5) FROM numbers(100);
OPTIMIZE TABLE t_lc_merge FINAL;
SELECT 'merged', count() FROM t_lc_merge WHERE s = 'p1_7';
SELECT 'merged', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_merge WHERE s = 'p1_7');
SELECT 'merged', count() FROM t_lc_merge WHERE s = 'p0_7';
SELECT 'merged', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_merge WHERE s = 'p0_7');
DROP TABLE t_lc_merge;

-- NULL is always in the dictionaries of LowCardinality(Nullable).
DROP TABLE IF EXISTS t_lc_null;
CREATE TABLE t_lc_null (id UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_null;
INSERT INTO t_lc_null SELECT number, if(number % 2, 'x', NULL) FROM numbers(100);
INSERT INTO t_lc_null SELECT number + 100, 'y' FROM numbers(100);
SELECT 'nullable equals', count() FROM t_lc_null WHERE s = 'y';
SELECT 'nullable equals', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_null WHERE s = 'y');
SELECT 'nullable is null', count() FROM t_lc_null WHERE s IS NULL;
SELECT 'nullable is null', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_null WHERE s IS NULL);
SELECT 'nullable coalesce', count() FROM t_lc_null WHERE coalesce(s, 'y') = 'y';
SELECT 'nullable coalesce', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_null WHERE coalesce(s, 'y') = 'y');
DROP TABLE t_lc_null;

-- When a dictionary overflows, the keys that do not fit are written next to the granules,
-- so the dictionaries do not contain all values, and the part is not skipped.
DROP TABLE IF EXISTS t_lc_overflow;
CREATE TABLE t_lc_overflow (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 10, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_overflow;
INSERT INTO t_lc_overflow SETTINGS low_cardinality_max_dictionary_size = 10 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_lc_overflow SETTINGS low_cardinality_max_dictionary_size = 10, low_cardinality_use_single_dictionary_for_part = 1 SELECT number + 100, toString(number + 100) FROM numbers(100);
INSERT INTO t_lc_overflow SETTINGS low_cardinality_max_dictionary_size = 1000, max_block_size = 1000 SELECT number + 200, toString(number + 200) FROM numbers(100);
SELECT 'overflow', count() FROM t_lc_overflow WHERE s IN ('99', '199', '299');
SELECT 'overflow', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_overflow WHERE s IN ('99', '199', '299'));
SELECT 'overflow', count() FROM t_lc_overflow WHERE s = '99';
SELECT 'overflow', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_overflow WHERE s = '99');
DROP TABLE t_lc_overflow;

-- Compact parts and parts without the marker are not skipped.
DROP TABLE IF EXISTS t_lc_compact;
CREATE TABLE t_lc_compact (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_compact;
INSERT INTO t_lc_compact SELECT number, 'a' FROM numbers(100);
INSERT INTO t_lc_compact SELECT number + 100, 'b' FROM numbers(100);
SELECT 'compact', count() FROM t_lc_compact WHERE s = 'b';
SELECT 'compact', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_compact WHERE s = 'b');
DROP TABLE t_lc_compact;

DROP TABLE IF EXISTS t_lc_no_marker;
CREATE TABLE t_lc_no_marker (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 0;
SYSTEM STOP MERGES t_lc_no_marker;
INSERT INTO t_lc_no_marker SELECT number, 'a' FROM numbers(100);
INSERT INTO t_lc_no_marker SELECT number + 100, 'b' FROM numbers(100);
SELECT 'no marker', count() FROM t_lc_no_marker WHERE s = 'b';
SELECT 'no marker', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_no_marker WHERE s = 'b');
DROP TABLE t_lc_no_marker;

-- With FINAL, a part without matching values can hold the latest version of a row.
DROP TABLE IF EXISTS t_lc_final;
CREATE TABLE t_lc_final (id UInt64, s LowCardinality(String)) ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_final;
INSERT INTO t_lc_final VALUES (1, 'old');
INSERT INTO t_lc_final VALUES (1, 'new');
SELECT 'final', count() FROM t_lc_final FINAL WHERE s = 'old';
SELECT 'final', count() FROM t_lc_final FINAL WHERE s = 'new';
DROP TABLE t_lc_final;
