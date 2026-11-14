-- A killed `MODIFY COLUMN` leaves parts that store a Nested member as `String` while the table says `Array`.
-- Merging such a part writes the converted member under the sizes shared by the group, so it must fail
-- instead of emptying or shifting the arrays of the other members.

SET mutations_sync = 0, alter_sync = 0;

SELECT '-- wide part, horizontal merge';
DROP TABLE IF EXISTS t_wide;
CREATE TABLE t_wide (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_wide VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_wide;
ALTER TABLE t_wide MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_wide' SYNC FORMAT Null;
INSERT INTO t_wide VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_wide;
OPTIMIZE TABLE t_wide FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_wide ORDER BY id;

SELECT '-- wide part, vertical merge settings';
DROP TABLE IF EXISTS t_vertical;
CREATE TABLE t_vertical (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0;
INSERT INTO t_vertical VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_vertical;
ALTER TABLE t_vertical MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_vertical' SYNC FORMAT Null;
INSERT INTO t_vertical VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_vertical;
OPTIMIZE TABLE t_vertical FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_vertical ORDER BY id;

SELECT '-- compact parts, then a merge into a wide part';
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000, min_bytes_for_full_part_storage = 0;
INSERT INTO t_compact VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_compact;
ALTER TABLE t_compact MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_compact' SYNC FORMAT Null;
INSERT INTO t_compact VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_compact;
OPTIMIZE TABLE t_compact FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_compact ORDER BY id;
ALTER TABLE t_compact MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
OPTIMIZE TABLE t_compact FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_compact ORDER BY id;

SELECT '-- converted member declared after the other members';
DROP TABLE IF EXISTS t_member_last;
CREATE TABLE t_member_last (id UInt64, `n.a` Array(UInt64), `n.c` Array(String), `n.x` String)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_member_last (id, `n.x`, `n.a`, `n.c`) VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_member_last;
ALTER TABLE t_member_last MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_member_last' SYNC FORMAT Null;
INSERT INTO t_member_last (id, `n.x`, `n.a`, `n.c`) VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_member_last;
OPTIMIZE TABLE t_member_last FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_member_last ORDER BY id;

SELECT '-- single part';
DROP TABLE IF EXISTS t_single_part;
CREATE TABLE t_single_part (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_single_part VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_single_part;
ALTER TABLE t_single_part MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_single_part' SYNC FORMAT Null;
SYSTEM START MERGES t_single_part;
OPTIMIZE TABLE t_single_part FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_single_part ORDER BY id;

SELECT '-- projection';
DROP TABLE IF EXISTS t_projection;
CREATE TABLE t_projection (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String),
    PROJECTION p (SELECT id, `n.x`, `n.a`, `n.c` ORDER BY `n.c`))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_projection VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_projection;
ALTER TABLE t_projection MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_projection' SYNC FORMAT Null;
INSERT INTO t_projection VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_projection;
OPTIMIZE TABLE t_projection FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, n.a, n.c, n.x FROM t_projection ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT id, n.a, n.c, n.x FROM t_projection ORDER BY n.c SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT '-- control: converted sizes match the other members';
DROP TABLE IF EXISTS t_sizes_match;
CREATE TABLE t_sizes_match (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_sizes_match VALUES (0, '[7,8]', [1, 2], ['p', 'q']), (1, '[9]', [3], ['r']);
SYSTEM STOP MERGES t_sizes_match;
ALTER TABLE t_sizes_match MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_sizes_match' SYNC FORMAT Null;
INSERT INTO t_sizes_match VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_sizes_match;
OPTIMIZE TABLE t_sizes_match FINAL;
SELECT id, n.a, n.c, n.x FROM t_sizes_match ORDER BY id;
SELECT count(), any(type) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sizes_match' AND active AND column = 'n.x';

SELECT '-- control: share_nested_offsets = 0';
DROP TABLE IF EXISTS t_no_shared_offsets;
CREATE TABLE t_no_shared_offsets (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 0;
INSERT INTO t_no_shared_offsets VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_no_shared_offsets;
ALTER TABLE t_no_shared_offsets MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_no_shared_offsets' SYNC FORMAT Null;
INSERT INTO t_no_shared_offsets VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_no_shared_offsets;
OPTIMIZE TABLE t_no_shared_offsets FINAL;
SELECT id, n.a, n.c, n.x FROM t_no_shared_offsets ORDER BY id;

SELECT '-- recovery: MODIFY COLUMN back to the stored type';
DROP TABLE IF EXISTS t_recovery;
CREATE TABLE t_recovery (id UInt64, `n.x` String, `n.a` Array(UInt64), `n.c` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 0;
INSERT INTO t_recovery VALUES (0, '[]', [1, 2], ['p', 'q']), (1, '[]', [3], ['r']);
SYSTEM STOP MERGES t_recovery;
ALTER TABLE t_recovery MODIFY COLUMN `n.x` Array(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_recovery' SYNC FORMAT Null;
INSERT INTO t_recovery VALUES (2, [4], [4], ['s']);
SYSTEM START MERGES t_recovery;
OPTIMIZE TABLE t_recovery FINAL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
ALTER TABLE t_recovery MODIFY COLUMN `n.x` String SETTINGS alter_sync = 2, mutations_sync = 2;
OPTIMIZE TABLE t_recovery FINAL;
SELECT id, n.a, n.c, n.x, toTypeName(n.x) FROM t_recovery ORDER BY id;

SELECT '-- control: ordinary Nested table keeps the vertical merge';
DROP TABLE IF EXISTS t_vertical_normal;
CREATE TABLE t_vertical_normal (id UInt64, `n.x` Array(UInt64), `n.a` Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0;
INSERT INTO t_vertical_normal VALUES (0, [1, 2], [3, 4]);
INSERT INTO t_vertical_normal VALUES (1, [5], [6]);
OPTIMIZE TABLE t_vertical_normal FINAL;
SELECT id, n.x, n.a FROM t_vertical_normal ORDER BY id;

SELECT '-- merge outcomes';
SYSTEM FLUSH LOGS part_log;
SELECT table, countIf(error = 190) > 0 AS failed, countIf(error = 0) > 0 AS merged, countIf(merge_algorithm = 'Vertical') > 0 AS vertical
FROM system.part_log WHERE database = currentDatabase() AND event_type = 'MergeParts' GROUP BY table ORDER BY table;

DROP TABLE t_wide;
DROP TABLE t_vertical;
DROP TABLE t_compact;
DROP TABLE t_member_last;
DROP TABLE t_single_part;
DROP TABLE t_projection;
DROP TABLE t_sizes_match;
DROP TABLE t_no_shared_offsets;
DROP TABLE t_recovery;
DROP TABLE t_vertical_normal;
