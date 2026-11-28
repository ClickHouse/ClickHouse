-- Parts are skipped by the dictionaries of `LowCardinality` columns.
-- Each query prints the result and the number of parts left to read after the index analysis.

SET use_low_cardinality_dictionaries_for_part_pruning = 1;
SET max_low_cardinality_dictionaries_bytes_for_part_pruning = 1048576;
SET optimize_use_implicit_projections = 0;
-- The parts are small, so allow reading the dictionaries for any number of marks to read.
SET min_marks_per_part_to_load_statistics = 0;

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;

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

-- Every dictionary contains the default value, but the parts where no row has it are skipped for it.
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
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
INSERT INTO t_lc_merge SELECT number, 'p' || toString(number % 2) || '_' || toString(number % 5) FROM numbers(100);
INSERT INTO t_lc_merge SELECT number + 100, 'p' || toString(number % 2) || '_' || toString(number % 5 + 5) FROM numbers(100);
OPTIMIZE TABLE t_lc_merge FINAL;
SELECT 'merged', count() FROM t_lc_merge WHERE s = 'p1_7';
SELECT 'merged', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_merge WHERE s = 'p1_7');
SELECT 'merged', count() FROM t_lc_merge WHERE s = 'p0_7';
SELECT 'merged', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_merge WHERE s = 'p0_7');
DROP TABLE t_lc_merge;

-- LowCardinality(Nullable) is pruned like by a skip index of type `set`.
DROP TABLE IF EXISTS t_lc_null;
CREATE TABLE t_lc_null (id UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_null;
INSERT INTO t_lc_null SELECT number, if(number % 2, 'x', NULL) FROM numbers(100);
INSERT INTO t_lc_null SELECT number + 100, 'y' FROM numbers(100);
SELECT 'nullable equals', count() FROM t_lc_null WHERE s = 'y';
SELECT 'nullable equals', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_null WHERE s = 'y');
SELECT 'nullable is null', count() FROM t_lc_null WHERE s IS NULL;
-- The number of parts is not checked: `optimize_functions_to_subcolumns` reads `s IS NULL` from the subcolumn `s.null`.
SELECT 'nullable coalesce', count() FROM t_lc_null WHERE coalesce(s, 'y') = 'y';
SELECT 'nullable coalesce', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_null WHERE coalesce(s, 'y') = 'y');
DROP TABLE t_lc_null;

-- The filter is evaluated on all values of the dictionaries, including the values of the rows that the query skips
-- by other indexes, so the parts are not skipped by a filter with functions that can throw on some values:
-- here `CAST` to a non-Nullable type would throw on `NULL`, and `toUInt32` on ''.
DROP TABLE IF EXISTS t_lc_unused;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_lc_unused (id UInt64, n LowCardinality(Nullable(Int32)), s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_unused;
INSERT INTO t_lc_unused SELECT number, number + 1, toString(number + 1) FROM numbers(10);
INSERT INTO t_lc_unused SELECT number + 10, number + 101, toString(number + 101) FROM numbers(10);
SELECT 'unused cast', count() FROM t_lc_unused WHERE CAST(CAST(n, 'LowCardinality(String)'), 'String') = '5';
SELECT 'unused conversion', count() FROM t_lc_unused WHERE toUInt32(s) = 105;
SELECT 'unused conversion', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_unused WHERE toUInt32(s) = 105);
INSERT INTO t_lc_unused VALUES (100, NULL, '');
SELECT 'skipped null', count() FROM t_lc_unused WHERE id < 10 AND CAST(CAST(n, 'LowCardinality(String)'), 'String') = '5';
-- Every dictionary has `NULL` and the default value, but the parts where the rows do not have them are skipped for them.
SELECT 'used null', count() FROM t_lc_unused WHERE n IS NULL;
SELECT 'used default', count() FROM t_lc_unused WHERE s = '';
SELECT 'used default', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_unused WHERE s = '');
DROP TABLE t_lc_unused;

-- When a dictionary overflows (more distinct values than `low_cardinality_max_dictionary_size`, 8192 by default),
-- the keys that do not fit are written next to the granules, so the dictionaries do not contain all values,
-- and the part is not skipped.
DROP TABLE IF EXISTS t_lc_overflow;
CREATE TABLE t_lc_overflow (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_overflow;
INSERT INTO t_lc_overflow SELECT number, toString(number) FROM numbers(20000);
INSERT INTO t_lc_overflow SELECT number + 20000, 'small' FROM numbers(100);
SELECT 'overflow', count() FROM t_lc_overflow WHERE s = 'zzz';
SELECT 'overflow', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_overflow WHERE s = 'zzz');
SELECT 'overflow', count() FROM t_lc_overflow WHERE s IN ('0', '8191', '8192', '19999', 'small');
SELECT 'overflow', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_overflow WHERE s = 'small');
DROP TABLE t_lc_overflow;

-- Compact parts and parts without the marker are not skipped.
DROP TABLE IF EXISTS t_lc_compact;
CREATE TABLE t_lc_compact (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 10000000000, min_rows_for_wide_part = 1000000000, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_compact;
INSERT INTO t_lc_compact SELECT number, 'a' FROM numbers(100);
INSERT INTO t_lc_compact SELECT number + 100, 'b' FROM numbers(100);
SELECT 'compact', count() FROM t_lc_compact WHERE s = 'b';
SELECT 'compact', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_compact WHERE s = 'b');
DROP TABLE t_lc_compact;

DROP TABLE IF EXISTS t_lc_no_marker;
CREATE TABLE t_lc_no_marker (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 0;
SYSTEM STOP MERGES t_lc_no_marker;
INSERT INTO t_lc_no_marker SELECT number, 'a' FROM numbers(100);
INSERT INTO t_lc_no_marker SELECT number + 100, 'b' FROM numbers(100);
SELECT 'no marker', count() FROM t_lc_no_marker WHERE s = 'b';
SELECT 'no marker', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_no_marker WHERE s = 'b');
DROP TABLE t_lc_no_marker;

-- With FINAL, a part without matching values can hold the latest version of a row.
DROP TABLE IF EXISTS t_lc_final;
CREATE TABLE t_lc_final (id UInt64, s LowCardinality(String)) ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1;
SYSTEM STOP MERGES t_lc_final;
INSERT INTO t_lc_final VALUES (1, 'old');
INSERT INTO t_lc_final VALUES (1, 'new');
SELECT 'final', count() FROM t_lc_final FINAL WHERE s = 'old';
SELECT 'final', count() FROM t_lc_final FINAL WHERE s = 'new';
DROP TABLE t_lc_final;
