-- Random settings limits: enable_writes_to_columns_cache=(0, 0)
-- A lightweight update or delete stored in a patch part is applied to the rows matched by the sorting key values read from
-- the part and from the patch part, so `apply_string_filters_during_scan` must not replace the non-matching key values.

DROP TABLE IF EXISTS t_string_filter_patch;
CREATE TABLE t_string_filter_patch (k String, v Int64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';
SYSTEM STOP MERGES t_string_filter_patch;
INSERT INTO t_string_filter_patch SELECT leftPad(toString(number), 6, '0'), number FROM numbers(1000);
SET apply_string_filters_during_scan = 1, lightweight_delete_mode = 'lightweight_update';

-- An update of a column read in the PREWHERE step that applies the patch.
UPDATE t_string_filter_patch SET v = -1 WHERE k = '000935';
SELECT k, v FROM t_string_filter_patch PREWHERE v = -1 AND k LIKE '%35';

-- The deleted key matches the filter.
DELETE FROM t_string_filter_patch WHERE k = '000035';
SELECT k FROM t_string_filter_patch PREWHERE k LIKE '%35' ORDER BY k;

-- Most deleted keys do not match the filter, so the key column of the patch part is filtered too.
DELETE FROM t_string_filter_patch WHERE k < '000500';
SELECT k FROM t_string_filter_patch PREWHERE endsWith(k, '35') ORDER BY k;

DROP TABLE t_string_filter_patch;
