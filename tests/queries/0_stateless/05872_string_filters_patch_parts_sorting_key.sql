-- Tags: no-replicated-database
-- no-replicated-database: fails due to additional shard.

-- The scan-time string filter (`apply_string_filters_during_scan`) replaces non-matching values with
-- empty strings, and the row is rejected only after PREWHERE has been evaluated. Patch parts in the
-- `MergeOnKey` mode are applied before PREWHERE and matched to the rows by the sorting key values,
-- so the substituted values of the sorting key columns would make the patches miss their rows.

SET enable_lightweight_update = 1;
SET mutations_sync = 2, lightweight_deletes_sync = 2;
SET apply_string_filters_during_scan = 1, optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t_string_filter_patch_wide;
DROP TABLE IF EXISTS t_string_filter_patch_compact;

CREATE TABLE t_string_filter_patch_wide (k String, v Int64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_string_filter_patch_compact (k String, v Int64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;

SYSTEM STOP MERGES t_string_filter_patch_wide;
SYSTEM STOP MERGES t_string_filter_patch_compact;

INSERT INTO t_string_filter_patch_wide SELECT leftPad(toString(number), 6, '0'), number FROM numbers(1000);
INSERT INTO t_string_filter_patch_wide SELECT leftPad(toString(number), 6, '0'), number FROM numbers(1000, 1000);
INSERT INTO t_string_filter_patch_compact SELECT leftPad(toString(number), 6, '0'), number FROM numbers(1000);
INSERT INTO t_string_filter_patch_compact SELECT leftPad(toString(number), 6, '0'), number FROM numbers(1000, 1000);

DELETE FROM t_string_filter_patch_wide WHERE k = '000035' SETTINGS lightweight_delete_mode = 'lightweight_update';
DELETE FROM t_string_filter_patch_wide WHERE k = '001235' SETTINGS lightweight_delete_mode = 'lightweight_update';
UPDATE t_string_filter_patch_wide SET v = -1 WHERE k = '000135' OR k = '001335';

DELETE FROM t_string_filter_patch_compact WHERE k = '000035' SETTINGS lightweight_delete_mode = 'lightweight_update';
DELETE FROM t_string_filter_patch_compact WHERE k = '001235' SETTINGS lightweight_delete_mode = 'lightweight_update';
UPDATE t_string_filter_patch_compact SET v = -1 WHERE k = '000135' OR k = '001335';

SELECT 'wide';
SELECT count(), groupArrayIf(k, k IN ('000035', '001235')) FROM t_string_filter_patch_wide WHERE k LIKE '%35';
SELECT count() FROM t_string_filter_patch_wide WHERE endsWith(k, '35');
SELECT count() FROM t_string_filter_patch_wide PREWHERE k LIKE '%35';
SELECT count() FROM t_string_filter_patch_wide WHERE k LIKE '%35' SETTINGS apply_string_filters_during_scan = 0;
SELECT k, v FROM t_string_filter_patch_wide WHERE k LIKE '%35' AND v < 0 ORDER BY k;

SELECT 'compact';
SELECT count(), groupArrayIf(k, k IN ('000035', '001235')) FROM t_string_filter_patch_compact WHERE k LIKE '%35';
SELECT count() FROM t_string_filter_patch_compact WHERE endsWith(k, '35');
SELECT count() FROM t_string_filter_patch_compact PREWHERE k LIKE '%35';
SELECT count() FROM t_string_filter_patch_compact WHERE k LIKE '%35' SETTINGS apply_string_filters_during_scan = 0;
SELECT k, v FROM t_string_filter_patch_compact WHERE k LIKE '%35' AND v < 0 ORDER BY k;

DROP TABLE t_string_filter_patch_wide;
DROP TABLE t_string_filter_patch_compact;
